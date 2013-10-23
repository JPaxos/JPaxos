sort $1 -n | awk \
'
 function printLine(){
   mean=sum/cnt;
   stdev=0;
   if(cnt>1)
   {
     for(i=1;i<=cnt;++i)
       stdev+=(mean-vals[i])*(mean-vals[i]);
     stdev=sqrt(stdev)/cnt-1;
   }
   printf("%s %f %f %d\n",last,mean,stdev,cnt);
 }
 BEGIN   {last=-1; sum=.0; cnt=0}
 /^#/
 /^$/
 /^[^#]/ {
           if(last==$1)
           {
             cnt++;
             sum+=$2;
             vals[cnt]=$2;
           }
           else
           {
             if(last!=-1)
               printLine();
             sum=.0;
             cnt=1;
             sum+=$2;
             vals[cnt]=$2;
           }
           last=$1
         }
 END     {printLine()}
'
