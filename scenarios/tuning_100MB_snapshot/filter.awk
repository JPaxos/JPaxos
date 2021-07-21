{
	a=$8;
	b=$20;
	c=$32;

	da=3*a/(a+b+c+0.001);
	db=3*b/(a+b+c+0.001);
	dc=3*c/(a+b+c+0.001);

	if(da>1.05||da<0.95) next;
	if(db>1.05||db<0.95) next;
	if(dc>1.05||dc<0.95) next;

	#if(da <= 1.05 && da >= 0.95 &&
	#   db <= 1.05 && db >= 0.95 &&
	#   dc <= 1.05 && dc >= 0.95 )
	#   next;

	print $0;
}
