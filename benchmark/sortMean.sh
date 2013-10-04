sort $1 -n | awk \
'
 BEGIN   {last=-1; sum=.0; cnt=0}
 /^#/
 /^$/
 /^[^#]/ {
          if(last==$1)
             {cnt++;sum+=$2}
          else
             if(last!=-1)
                {printf("%s %f\n",last,sum/cnt);sum=.0;cnt=0}
          last=$1
         }
 END     {printf("%s %f\n",last,sum/cnt);}
'
