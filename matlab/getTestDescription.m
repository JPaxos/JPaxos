function [ n cNodes testLength reqSize nClients wsz bsz] = getTestDescription( )
%GETTESTDESCRIPTION Summary of this function goes here
%   Detailed explanation goes here
fid = fopen('conf.txt');
settings = textscan(fid, '%s %s', ...
    'delimiter', '=',  ...
    'MultipleDelimsAsOne', 1);
fclose(fid);
n=str2double(settings{2}{1});
nClients = strread(settings{2}{2});
wsz=strread(settings{2}{3});
bsz=strread(settings{2}{4});
cNodes=str2double(settings{2}{5});
testLength=str2double(settings{2}{6});
reqSize=str2double(settings{2}{7});
%reqSize=-1;

% testDesc = sprintf('n=%d, reqSz=%d, d=%d',...
%         nReplicas, reqSize, testDuration);
% testDesc = sprintf('[n=%d, c=%d(%dx%d), reqSz=%d, d=%d]',...
%         nReplicas, clientsPerNode*clientNodes,...
%         clientNodes, clientsPerNode,...
%         reqSize, testDuration);
end

