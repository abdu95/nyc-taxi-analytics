def majorityElement(nums):
    result = {}
    for item in nums:
        if item not in result:
            result[item] = 1
        else: 
            result[item] += 1
    
    for key in result:
        if result[key] > len(result)/2:
            return key


input = [8,8,7,7,7]
result = majorityElement(input)
print(result)