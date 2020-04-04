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