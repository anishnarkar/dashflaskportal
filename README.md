# dashflaskportal
Portal for uploading and visualizing data



# File Contraints

Uploading Mechanism: Uploading mechanism takes data in the FLA toolkit and divides it into 7 SQL tables on Azure. Considering the structure of the toolkit, it is mandatory that certain rules regarding the structure of the file can’t be tampered with for smooth functioning of the mechanism. 
1.	The order of sheet in the toolkit should follow a pre-defined sequence. The first two sheets should contain meta data about the toolkit and the 7 subsequent sheets should contain data (Sheets 3-9).
2.	The order of sheets within the above range must also stay constant, the following is the order which we have used to generate the mechanism:
1.	Factory Information
2.	Occupations
3.	Pay-Period-Peak
4.	Pay-Period-Regular
5.	Pay-Period-Low
6.	In-Kind Benefits
7.	Cash Benefits
3.	The sheets beyond 9th sheet (Cash Benefits) does not affect the uploading mechanism. 
4.	The structure of each sheet (Position of sheet elements) should stay the same as the samples provided on Dropbox.

SQL tables have predefined data types which prohibits inserting data with different data types. The following are the constraints for data in tables of the toolkit arranged by the sheets in which they appear:
1.	Factory Information
1.	Demographic Information: Only Integers
2.	Production Information: Ensure Output is a integer and no suffix such as Pcs is added. 
3.	Rest Days: Only Integers
4.	All other data points have no constraints
2.	Occupation: No constraints
3.	Pay Period(Peak, Regular, Low)
1.	Pay- Period Information: Regular working hours and regular working days should only have numeric data with no suffix of any sort
2.	Worker Information, Hours, Hourly Pay- All the data points should be numeric with no suffix
3.	Incentive Pay:  Total Incentive Pay Should be numeric with no suffix
4.	Legal Deduction and Taxes: Total Tax Deduction should be numeric with no suffix
5.	In-Kind Benefits, Cash Benefits: Data Entry for “Total Amount Spent Annually” should be numeric without any suffix or prefix
Uploading mechanism considers blank values for the above-mentioned columns as 0
