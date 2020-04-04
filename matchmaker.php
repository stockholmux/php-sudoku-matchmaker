<?php


if(!extension_loaded('swoole')){
  throw new Exception("install swoole extension, pecl install swoole");
}

const COMMIT_PARTITIONS = 3;
const POINTS_STREAM_PARTITIONS = COMMIT_PARTITIONS;
const GAME_STREAM_PARTITIONS = COMMIT_PARTITIONS;
const GROUP_NAME = "matchmaking";
const KEYNS = 'mm:';
const MUST_LOCK_P1 = 'MUST_LOCK_P1';
const MUST_LOCK_P2 = 'MUST_LOCK_P2';
const MUST_FAIL_MATCH = 'MUST_FAIL_MATCH';

const GAMESTATE_READY = 'READY';
const GAMESTATE_ENDED = 'ENDED';

function sequentialToAssociativeArray($sequentialArray) {
  $out = [];
  foreach ($sequentialArray as $key=>$value) {
    if ($key % 2 == 0) {
      $out[$value] = $sequentialArray[$key+1];
    }
  }
  return $out;
}
function add_slot($key,$slot) {
  return "$key-{$slot}";
}

class RedisPool
{
    /**@var \Swoole\Coroutine\Channel */
    protected $pool;

    /**
     * RedisPool constructor.
     * @param int $size max connections
     */
    public function __construct(int $size = 100)
    {
        $this->pool = new \Swoole\Coroutine\Channel($size);
        for ($i = 0; $i < $size; $i++) {
            $redis = new \Swoole\Coroutine\Redis();
            $res = $redis->connect('127.0.0.1', 6379);
            if ($res == false) {
                throw new \RuntimeException("failed to connect redis server.");
            } else {
                $this->put($redis);
            }
        }
    }

    public function get(): \Swoole\Coroutine\Redis
    {
        return $this->pool->pop();
    }

    public function put(\Swoole\Coroutine\Redis $redis)
    {
        $this->pool->push($redis);
    }

    public function close(): void
    {
        $this->pool->close();
        $this->pool = null;
    }
}

function readGroupLua($group_name) {
  return <<<EOT
  local busyCheck = redis.pcall('XGROUP','CREATE',KEYS[1],'$group_name',0,'MKSTREAM');
  local busyGroup = 'BUSYGROUP';
  if type(busyCheck) ~= 'table' then
    return 'OK'
  elseif type(busyCheck) == 'table' and busyCheck.err and busyCheck.err:sub(1,#busyGroup) == busyGroup then
    return 'OK' -- ignore BUSYGROUP
  else 
    return busyCheck.err
  end
EOT;
}

function lock() {
  return <<<EOT
  local lock_key = KEYS[1]
  local lock_id = ARGV[1]

  local lock = redis.call("GET", lock_key)
  if not lock then
    redis.call("SET", lock_key, lock_id)
    return 1
  end

  if lock == lock_id then
    return 1
  end

  return 0
EOT;
}

function unlock() {
  return <<<EOT
  local lock_key = KEYS[1]
  local lock_id = ARGV[1]

  local lock = redis.call("GET", lock_key)
  if lock == lock_id then
    redis.call("DEL", lock_key)
  end
EOT;
}

function update_match_key($redis,$match_key,$p1,$p2,$game,$new_state) {
  if ($redis->request(['HSET',$match_key,'p1',$p1,'p2',$p2,'game',$game,'state',$new_state]) == FALSE) {
    throw new Exception("Error updating match: $$match_key");
  }
}
function ack_entry($redis,$commit_stream,$entry_id) {
  if ($redis->request(['XACK',$commit_stream,GROUP_NAME,$entry_id]) == FALSE) {
    throw new Exception("Error ack'ing: $commit_stream / $entry_id");
  }
}
function update_matches_set($redis,$op,$p,$match_key) {
  $p_key = KEYNS . "player:$p:matches";
  if ($redis->request(['s'.$op,$p_key, $match_key]) == FALSE) {
    throw new Exception("Error $op update matches set: $p_key / $match_key");
  }
}
function stream_add_action($redis,$key,$match_id,$p1,$p2,$game,$action) {
  if ($redis->request([
    'XADD',$key,'*',
    'match_id',$match_id,
    'p1',$p1, 
    'p2',$p2, 
    'game', $game, 
    'action',$new_action
  ]) == FALSE) {
    throw new Exception("Error XADD'ing to $key");
  }
}

function unlockPlayers($redis,$p1,$p2,$match_id) {
    $p1_lock = KEYNS . "player:$p1:lock";
    $p2_lock = KEYNS . "player:$p2:lock"; 
    if ($redis->request(['EVAL',unlock(),1,$p1_lock, $match_id])) {
      throw new Exception("Error while trying to unlock player $p1 on $match_id");
    }
    if ($redis->request(['EVAL',unlock(),1,$p2_lock, $match_id])) {
      throw new Exception("Error while trying to unlock player $p2 on $match_id");
    }
}

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


function process_match($redis, $commit_stream, $entry_id, $match) {
  var_dump($match);
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
      ack_entry($commit_stream,$entry_id);
      return;
    }
    //Create a corresponding match key
    $new_state = "WAITING_P2";
    update_match_key($redis,$match_key,$p1,$p2,$game,$new_state);

    //Add the match to the list of pending matches for p1 and p2
    //(this is necessary to keep clients informed about pending invites)
    update_matches_set($redis,'ADD',$p1,$match_key);
    update_matches_set($redis,'ADD',$p2,$match_key);

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
      stream_add_action($redis,$partition,$match_id,$p1,$p2,$game,$new_action);
      ack_entry($redis,$commit_stream,$entry_id);
      return;
    }

    // Set the match to the next state 
    $new_state = "WAITING_GAME";
    $redis->request(['HSET',$match_key, 'state', $new_state]);
    //todo add error checking

    // Publish that the match is ready for consumption by the corresponding game engine
    $partition = add_slot(KEYNS . 'matches', rand(0, COMMIT_PARTITIONS));
    stream_add_action($redis,$partition,$match_id,$p1,$p2,$game,$new_action);
    ack_entry($redis,$commit_stream,$entry_id);
  } elseif ($action == MUST_FAIL_MATCH) {
    $new_state = "FAILED";
    update_match_key($redis,$match_key,$p1,$p2,$game,$new_state);

    // Undo the lock for both players
    unlockPlayers($redis,$p1,$p2,$match_id);



    update_matches_set($redis,'REM',$p1,$match_key);
    update_matches_set($redis,'REM',$p2,$match_key);

    // Ack!
    ack_entry($redis,$commit_stream,$entry_id);
  } else {
    throw new Exception("Unknown action: $action");
  }

}