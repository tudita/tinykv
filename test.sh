#!/bin/bash
for ((i=4;i<=10;i++));
do
	echo "ROUND $i";
	make project3b > ./out/out-$i.txt;
done

