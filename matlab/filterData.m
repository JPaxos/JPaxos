function data = filterData(data)
if ~isempty(data)    
	% Discard the first 10% of the data. Column 2 
    % still contains absolute values
	n = length(data(:,1));
	cutoffTime = data(1,2) + (data(n,2)-data(1,2))/10;
	% find the first index that is greater than the cutoff time
	cutoffIndex = find(data(:,2)>cutoffTime, 1, 'first');
	data = data(cutoffIndex:n,:);
    
    % Remove any consensus instances that were not finished. They have
    % negative values on the duration column (3rd)    
    ind = data(:,3) > 0;
    data = data(ind, :);
end