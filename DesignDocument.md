## Design Document for Employee Tracking system 
 
### Requirement :  
* Generate the employee login, logout data.  
* Employee can do multiple login and logout in a day 
Calculate the following attribute. 
* Average working time of each employee per week, per month, per 2month, per 3month 
* Average absent time of each employee per week, per month,  per 2month, per 3month 
* Average Expected arrival time of each employee  per month,  per 2month, per 3month 
 
### Solution : 
 Generate the Employee data in such a way that All user did not came all days and save the data in following format :  
 
Sample Data Format : 
```json
[ 
  { "Emp_id" : 31808, "time": "2016-11-26 9:30 am", "type": "inTime" },  
  { "Emp_id" : 31808, "time": "2016-11-26 10:30 am", "type": "outTime" },  
  { "Emp_id" : 31808, "time": "2016-11-26 11:30 am", "type": "inTime" },  
  { "Emp_id" : 31808, "time": "2016-11-26 06:30 pm", "type": "outTime" }  
] 
```
###Algorithm to generate data
There are following steps to generate login, logout data of a employee in a day :  
```
Steps 1: choose the  minArrivalTime = 9 , maxArrivalTime = 11 , maxLogoutTime = 18, loginTime = 0, logoutTime = 0; 
<!-- For each employee, each day login, logout time generation -->                                
Step 2: generate random loginTime = Random(minArrivalTime , maxArrivalTime )                                   
Step 3: if(logoutTime - loginTime) > 2hr then repeat Step2                                                   
Step 4: generate random logoutTime = Random(maxArrivalTime , maxLogoutTime)                                       
Step 4: update minArrivalTime and maxArrivalTime values: minArrivalTime = logoutTime, maxArrivalTime = maxLogoutTime       
Step 6: create employee data object using employee id and calculated login and Logout Time                          
Step 7: repeat 2 to 6 steps while(maxArrivalTime + constant (lets assume 20 mins, because probability of coming exact      maxLogoutTime is very less ) <=  maxLogoutTime)                                             
Step 8: Save generated data into json file.                                                            
<!-- Repeat the above steps to generate data for multiple day -->    
```
  
Read the JSON file and calculate the total working time, and total absent time of each user and save it in following format 
Sample Schema to save data for all month:  


Identifier  | Total_Working_Time | Total_Absent_Time |
 ------------ | :-----------: | -----------: |
emp_01       |       108     |      12      |
emp_02       |       100     |      20      |                                                                  
 
### Calculation for per employee :                                                      
* Average working time/week = total_working_Time/ no of weeks                                          
* Average working time/month = total_working_Time/ no of month                                                    
* Average absent time/week = total_absent_Time/ no of weeks                                                       
* Average absent time/month = total_absent_Time/ no of month  

### Calculation for all Employee :                                      
* Average working time/week = total_working_Time for all employee/ no of weeks                                   
* Average working time/month = total_working_Time for all employee/ no of month 
* Average absent time/week = total_absent_Time for all employee/ no of weeks 
* Average absent time/month = total_absent_Time for all employee/ no of month                                       
 
 
* To calculate the average expected arrival time, take arrival time of all day and  and sort it and take median.  
***Question:*** How to save all user arrival time for all days. What should be the schema for that? (Stuck) 
  
  
 
