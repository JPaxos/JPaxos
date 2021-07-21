#!/bin/bash

num=0;
while [[ -f "log.$num" ]]
do
	let num++
done

cat - > "log.$num"
