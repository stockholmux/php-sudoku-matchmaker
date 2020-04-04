<?php
require 'matchmaker.php';


if ($argc == 1) {
  echo("Usage: ./game_worker.php <uniqueInstanceID>\r\n");
  exit();
}
$instanceID = $argv[1];

Swoole\Runtime::enableCoroutine();

go(function() use ($instanceID) {
  $pool = new RedisPool();
  for ($slot = 0; $slot < COMMIT_PARTITIONS; $slot++) {
    go(function() use ($pool,$instanceID,$slot) {
      $redis = $pool->get();
      $stream_name = add_slot(KEYNS . 'sudoku-engine:games',$slot);
      echo "Listening entries at stream key `$stream_name`\r\n";
      process_game_partition($redis, $stream_name, $instanceID);
      $pool->put($redis);
    });
  }
});
function process_game($redis, $commit_stream, $msg_id, $msg) {
  $match_id = $msg['mid'];
  $game_state = $msg['state'];
  
  $match_key = KEYNS . "match:$match_id";
  if ($game_state == GAMESTATE_READY) {
    $new_state = GAMESTATE_READY;

    if ($redis->request(['HSET',$match_key, 'state', $new_state]) === FALSE) {
      throw new Exception("Error updating state: $$match_key to $new_state");
    }

    ack_entry($redis,$commit_stream,$msg_id);
  } elseif ($game_state == GAMESTATE_ENDED) {
    $match = $redis->request(['HGETALL',$match_key]);
    if ($match === FALSE) {
      throw new Exception("Could not get match $$match_key");
    }
    $p1 = $match["p1"];
		$p2 = $match["p2"];
    $game = $match["game"];
    
    $new_state = GAMESTATE_ENDED;

    if ($redis->request(['HSET',$match_key, 'state', $new_state]) === FALSE) {
      throw new Exception("Error updating match: $$match_key");
    }

    // Undo the lock for both players
    unlockPlayers($redis,$p1,$p2,$match_id);
    
    // Remove the match from each player's list
    update_matches_set($redis,'REM',$p1,$match_key);
    update_matches_set($redis,'REM',$p2,$match_key);

    // Publish the new points accruded by the players
    // points are a placeholder for now
    $partition = add_slot(KEYNS . 'points', rand(0, POINTS_STREAM_PARTITIONS));
    if ($redis->request(['XADD',$partition, '*', 'p1', $p1, 'p2', $p2, 'game', $game, 'points', 250 ]) === FALSE) {
      throw new Exception("Error adding point stream entry on partition $partition");
    }
    
    ack_entry($redis,$commit_stream,$msg_id);
  } else {
    throw new Exception("Unknown gamestate: $game_state");
  }
}

function process_game_partition($redis, $stream_name, $instanceID) {
  $streamExists =  $redis->request(['EVAL',readGroupLua(GROUP_NAME),1,$stream_name]);
  if ($streamExists != "OK") {
    echo 'Warning: Busy Group \r\n';
  }
  $cmdArgs = ['XREADGROUP','GROUP', GROUP_NAME, $instanceID ]; 
  $streamsArgs = ['STREAMS', $stream_name];
  $stream = $redis->request(array_merge($cmdArgs,$streamsArgs,[0]));
  if ($stream != false) {
    while (true) {
      if (!is_null($stream)) {
        $streamByKey = $stream[0];
        $streamEntries = $streamByKey[1];
        foreach ($streamEntries as $entry) {
          $entryId = $entry[0];
          $entryFields = sequentialToAssociativeArray($entry[1]);

          process_game($redis, $stream_name, $entryId,$entryFields);
        }
      }
      // Ask to be assigned more items
      $stream = $redis->request(
        array_merge($cmdArgs,['COUNT',10,'BLOCK',6000],$streamsArgs,['>'])
      );
    }
  }
}