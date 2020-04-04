<?php 
require 'matchmaker.php';

if(!extension_loaded('redis')){
  throw new Exception("install redis extension, pecl install redis");
}

if ($argc !== 4) {
  echo("Usage: php ./main.php <port> <redis host> <redis port>\r\n");
  exit();
}
$port = $argv[1];
$redisHost = $argv[2];
$redisPort = $argv[3];

$http = new Swoole\HTTP\Server("127.0.0.1", $port);
$redis = new Redis();
$redis->connect($redisHost, $redisPort);
//$redis->auth('foobared');

$http->on('request', function ($request, $response) use ($redis) {
  
  //var_dump($request);

  $uri = $request->server['request_uri'];
  echo "URI: $uri \r\n";
  if ($uri === '/create_match') {
    $p1 = $request->get['p1'];
    $p2 = $request->get['p2'];
    if ((strlen($p1) > 0) and (strlen($p2) > 0)) {
      $commit_partition_number = rand(0, COMMIT_PARTITIONS);
      echo "partition number $commit_partition_number\r\n";
      $partition = add_slot(KEYNS . "commit", $commit_partition_number);
      $match_id = $redis->xAdd($partition, '*', ['p1' => $p1, 'p2' => $p2, 'game' => 'sudoku', 'action' => MUST_LOCK_P1]);
      $full_match_id = "$match_id-{{$commit_partition_number}}";
      $response->header('Content-Type', 'application/json');
      $response->end("{\"match_id\": \"$full_match_id\" }");

    } else {
      $response->status(400);
      $response->end("Request invalid");
    }
  } elseif ($uri === '/accept_match') {
    //FYI - not fully implemented elsewhere
    $match_id = $request->get['mid'];
    if (strlen($match_id) > 0 ) {
      $match_key = KEYNS . "match:" . $match_id;
      $match = $redis->hgetall($match_key);
      $state = $match["state"];
      if ($state == "WAITING_P2") {
        // Choose a random partition of the commit stream to update
        $partition = add_slot(KEYNS . "commit", rand(0, COMMIT_PARTITIONS));
        $new_action = MUST_LOCK_P2;
        $redis->xAdd($partition, '*', ['match_id'=> $match_id, 'p1'=>$match['p1'], 'p2'=>$match['p2'], 'game'=>$match['game'], 'action'=> $new_action]);
      }
      $response->header('Content-Type', 'application/json');
      $response->end("{\"ok\": True}");
    } else {
      $response->status(400);
      $response->end("Request invalid");
    }

  } else {
    $response->status(404);
    $response->end("No valid route");
  }
});

$http->on('start', function ($server) use ($port) {
  echo "Server has been started at port $port!\n";
});

$http->start();