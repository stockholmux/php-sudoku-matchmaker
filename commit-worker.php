<?php
require 'matchmaker.php';

if ($argc == 1) {
  echo("Usage: php ./commit_worker.php <uniqueInstanceID>\r\n");
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
        echo 'Warning: Busy Group\r\n';
      }
      echo "Listening entries at stream key `$stream_name`\r\n";
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
              echo "$streamByKey / $entryId\r\n";
              $entryFields = sequentialToAssociativeArray($entry[1]);
              process_match($redis, $commit_name, $entry_id, $streamEntries);
            }      
          } 
          // Ask for more matches
          $stream = $redis->request(
            array_merge($cmdArgs,['COUNT',10,'BLOCK',6000],$streamsArgs,['>'])
          );
        }
      }
      $pool->put($redis);
    });
  }
});
