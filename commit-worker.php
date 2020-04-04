<?php
require 'matchmaker.php';


if ($argc == 1) {
  echo("Usage: ./commit_worker.php <uniqueInstanceID>\r\n");
  exit();
}
$instanceID = $argv[1];


Swoole\Runtime::enableCoroutine();

go(function() use ($instanceID) {
  $pool = new RedisPool();
  for ($slot = 0; $slot < COMMIT_PARTITIONS; $slot++) {
    go(function() use ($pool,$instanceID,$slot) {
      $redis = $pool->get();
      $stream_name = add_slot(KEYNS . 'commit',$slot);
      $streamExists =  $redis->request(['EVAL',readGroupLua(GROUP_NAME),1,$stream_name]);
      if ($streamExists != "OK") {
        echo 'Warning: Busy Group';
      }
      $cmdArgs = ['XREADGROUP','GROUP', GROUP_NAME, $instanceID ]; 
      $streamsArgs = ['STREAMS', $stream_name];
      $stream = $redis->request(array_merge($cmdArgs,$streamsArgs,[0]));
      if ($stream != false) {
        while (true) {
          if (!is_null($stream)) {
            foreach ($stream as $message) {
              $key = $message[0];
              $fields = $message[1];
              echo "Message = ";
              var_dump($fields);
            }        
          } 

          $stream = $redis->request(
            array_merge($cmdArgs,['COUNT',10,'BLOCK',6000],$streamsArgs,['>'])
          );
        }
      }
      $pool->put($redis);
    });
  }
});



function process_match($redis, $commit_stream, $entry_id, $match) {
  $p1 = $match['p1'];
  $p2 = $match['p2'];
  $game = $match['game'];
  $match_id = $match['match_id'];
  $action = $match['action'];

  $match_key = KEYNS . "match:$match_id";

  if ($action == MUST_LOCK_P1) {
    $p1_lock = KEYNS . "player:$p1:lock";

    #lock P1
    $success =  $redis->request(['EVAL',lock(),1,$p1_lock,$match_id]);
    if ($success == false) {
      $new_action = MUST_FAIL_MATCH;
      $partition = add_slot(KEYNS . "commit", rand(0, COMMIT_PARTITIONS));
      //$redis->request(['XADD',$partition,'*','match_id',$match_id,'p1',$p1, 'p2',$p2, 'game', $game, 'action',$new_action]);
      stream_add_action($redis,$partition,$match_id,$p1,$p2,$game,$new_action);
      //$redis->request(['XACK',$commit_stream,GROUP_NAME, $entry_id]);
      ack_entry($commit_stream,$entry_id);
      return;
    }
    //Create a corresponding match key
    $new_state = "WAITING_P2";
    //self.redis.hmset(match_key, {"p1": p1, "p2": p2, "game": game, "state": new_state})
    //$redis->request(['HSET',$match_key,'p1',$p1,'p2',$p2,'game',$game,'state',$new_state]);
    update_match_key($redis,$match_key,$p1,$p2,$game,$new_state);

    //Add the match to the list of pending matches for p1 and p2
    //(this is necessary to keep clients informed about pending invites)
    /*$p1_matches_key = KEYNS . "player:$p1:matches";
    $p2_matches_key = KEYNS . "player:$p2:matches";
    $redis->request(['sadd',$p1_matches_key, $match_key]);
    $redis->request(['sadd',$p2_matches_key, $match_key]);*/
    update_matches_set($redis,'ADD',$p1,$match_key);
    update_matches_set($redis,'ADD',$p2,$match_key);

    //$redis->request(['XACK',$commit_stream,GROUP_NAME, $entry_id]);
    ack_entry($redis,$commit_stream,$entry_id);
    return;
  } elseif ($action == MUST_LOCK_P2) {
    // We must lock P2 and wait for sudoku-engine to have created the match
    $p2_lock = KEYNS . "player:$p2:lock";
    
    // Lock P2
    $success =  $redis->request(['EVAL',lock(),1,$p2_lock,$match_id]);
    //todo refactor DRY lock p1
    if ($success == false) {
      $new_action = MUST_FAIL_MATCH;
      $partition = add_slot(KEYNS . "commit", rand(0, COMMIT_PARTITIONS));
      //$redis->request(['XADD',$partition,'*','match_id',$match_id,'p1',$p1, 'p2',$p2, 'game', $game, 'action',$new_action]);
      stream_add_action($redis,$partition,$match_id,$p1,$p2,$game,$new_action);
      //$redis->request(['XACK',$commit_stream,GROUP_NAME, $entry_id]);
      ack_entry($redis,$commit_stream,$entry_id);
      return;
    }

    // Set the match to the next state 
    $new_state = "WAITING_GAME";
    $redis->request(['HSET',$match_key, 'state', $new_state]);
    //todo add error checking

    // Publish that the match is ready for consumption by the corresponding game engine
    $partition = add_slot(KEYNS . 'matches', rand(0, COMMIT_PARTITIONS));
    //$redis->request(['XADD', $partition, 'match_id', $match_id, 'p1', $p1, 'p2', $p2, 'game', $game, 'state', 'CREATED']);
    stream_add_action($redis,$partition,$match_id,$p1,$p2,$game,$new_action);
    //$redis->request(['XACK',$commit_stream,GROUP_NAME, $entry_id]);
    ack_entry($redis,$commit_stream,$entry_id);
  } elseif ($action == MUST_FAIL_MATCH) {
    $new_state = "FAILED";
    //$redis->request(['HSET',$match_key,'p1',$p1,'p2',$p2,'game',$game,'state',$new_state]);
    update_match_key($redis,$match_key,$p1,$p2,$game,$new_state);

    // Undo the lock for both players
    //$p1_lock = KEYNS . "player:$p1:lock";
    //$p2_lock = KEYNS . "player:$p2:lock"; 
    //$redis->request(['EVAL',unlock(),1,$p1_lock, $match_id]);
    //$redis->request(['EVAL',unlock(),1,$p2_lock, $match_id]);
    //todo error checking
    unlockPlayers($redis,$p1,$p2,$match_id);


    // Remove the match from each player's list
    /*$p1_matches_key = KEYNS . "player:$p1:matches";
    $p2_matches_key = KEYNS . "player:$p2:matches";
    $redis->request(['SREM',$p1_matches_key, $match_key]);
    $redis->request(['SREM',$p2_matches_key, $match_key]);*/
    update_matches_set($redis,'REM',$p1,$match_key);
    update_matches_set($redis,'REM',$p2,$match_key);
    //todo error checking

    // Ack!
    //$redis->request(['XACK',$commit_stream,GROUP_NAME, $entry_id]);
    ack_entry($redis,$commit_stream,$entry_id);
  } else {
    throw new Exception("Unknown action: $action");
  }

}
