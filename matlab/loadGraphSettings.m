% Use for inclusion on a paper or presentation
set(gca,'FontSize',24);
set(gca,'FontName','Times');

%o specify default values, create a string beginning with the word Default, 
% followed by the object type, and finally, by the object property. 
% For example, to specify a default value of 1.5 points for the line 
% property LineWidth at the level of the current figure, use the statement
% set(gcf,'DefaultLineLineWidth',1.5)

% line properties
set(0,'DefaultLineMarkerSize',16);
set(0,'DefaultLineLineWidth',2);
%set(0,'DefaultAxesLineStyleOrder',{'-'; '--'; '-.'; ':'});

%markerSize = 16;
%lineWidth = 2;
% cell array with the line spec for each line
% set(gcf,'DefaultAxesColorOrder',[1 0 0;0 0.5 0;0 0 1;0 0 0])

styles = {'-'; '--'; '-.'; ':';'-'; '--'; '-.'};
colors = [1 0 0;  0 0.5 0;  0 0 1;  0 0 0;  0.8 0.8 0;  1 0 1];
markers = ['+'; 's';'^';'x';'d';'p';];
%colors = {'-+'; ':s'; '-.^'; ':x'; '-d'; '-s'; '-p'};

% true for cluster, false for emulab
largeScale=false;