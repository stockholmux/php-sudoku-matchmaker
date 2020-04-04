<?php
require 'matchmaker.php';

if ($argc == 1) {
  echo("Usage: php ./game_worker.php <uniqueInstanceID>\r\n");
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