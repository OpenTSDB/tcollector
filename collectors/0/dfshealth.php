#!/usr/bin/php
<?php

$metrics = array(
  "capacity.configured" => "/Configured\sCapacity:\s(\d+)/",
  "capacity.present" => "/Present\sCapacity:\s(\d+)/",
  "capacity.remaining" => "/DFS\sRemaining:\s(\d+)/",
  "capacity.used" => "/DFS\sUsed:\s(\d+)/",
  "blocks.underreplicated" => "/Under\sreplicated\sblocks:\s(\d+)/",
  "blocks.with_corrupt_replicas" =>"/Blocks\swith\scorrupt\sreplicas:\s(\d+)/",
  "blocks.missing" => "/Missing\sblocks:\s(\d+)/",
  "datanodes.available" => "/Datanodes available: (\d+) \(\d+ total, \d+ dead\)/",
  "datanodes.total" => "/Datanodes available: \d+ \((\d+) total, \d+ dead\)/",
  "datanodes.dead" => "/Datanodes available: \d+ \(\d+ total, (\d+) dead\)/"
  );

while(true)
{
  $time = time();
  $dfsoutput = `hadoop dfsadmin -report`;
  $blocks = explode("\n\n", $dfsoutput);
 
  $summary = $blocks[0] . $blocks[1];
 
  foreach ($metrics as $metric => $regex)
  {
    if (preg_match($regex, $summary, $match))
    {
      echo "hadoop.dfs." . $metric . " " . $time . " " . $match[1] . "\n";
    }
  }
  sleep(30);
}