select 
 (requ/100)/10.0 t, 
 -avg(prop) "Time spent before propose", 
 avg(deci)  "Paxos voting duration", 
 avg(stEx)  "Start of request execution",
 avg(enEx)  "End of request execution",
 avg(answ)  "Scheduling sending a reply",
 count(*)   "Throughput [r/s]"
from requestStats
where t>=5
group by t
order by t;