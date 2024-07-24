#!/bin/bash
for ((i=1;i<=20;i++));
do
	echo "ROUND $i";
	make project2c > ./out/out-$i.txt;
done

