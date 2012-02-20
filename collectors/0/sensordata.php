#!/usr/bin/php
<?php

while(true)
{
  $time = time();
  $sensordata = `sensors`;
  $sensordata = explode("\n", $sensordata);

  $temp = array();

  for($i = 0; $i < count($sensordata); $i++)
  {
    if (preg_match('/(.*):\s*([+-\d\.]+)/', $sensordata[$i], $match))
    {
      $temp[$match[1]] = floatval($match [2]);
    }
  }

  foreach ($temp as $metric => $value)
  {
    $tags = "";
    $metric = strtr($metric, array(" " => "_", '+' => ''));
     
    if (preg_match('/Core_(\d+)/', $metric, $match)) 
    {
      $tags = "core=" . $match[1];
      $metric = "coretemp";
    }
     
    if (preg_match('/fan(\d+)/', $metric, $match))
    {
      $tags = "fan=" . $match[1];
      $metric = "fanspeed";
    }

    echo "sensors." . $metric . " " . $time . " " . $value . " " . $tags . "\n";
  }

  sleep(10);
}