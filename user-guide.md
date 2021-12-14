
`                                                                                                                                                                                            `**swish-utilities User Guide**

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_


Swish Utilities for ServiceNow data retrieval (swish-utilities)


Introduction

Extraction of data from ServiceNow can be challenging. In some cases our users can retrieve the required data from ServiceNow by themselves. In other cases, particularly for Sys\_Audit extraction, auxiliary utility is required. 

For some companies, masking of the data is required before transferring to DeepCoding. The swish-utilities command line interface (CLI) is a tool for retrieval of data from ServiceNow ITSM system and for data masking.

For initial DeepCoding on-boarding of a company that is using ServiceNow, extraction of the following tables may be required:

- Incidents [incident]
- Change Requests [change\_request]
- Catalog Tasks [sc\_task]
- Sys Audit [sys\_audit]
- Schedule Entry [cmn\_schedule\_span]
- Group Members [sys\_user\_grmember]
- Schedule Entry [cmn\_schedule\_span]

The following are optional for sharing with DeepCoding (depending on company policy):

- Users [sys\_user]  


For a more detailed list, see the *Extracting ServiceNow Data* document.  



**Pre-Requisites**  

- The latest swish-utilities product should be downloaded. 
- swish-utilities can be executed only on Windows machines.
- Minimal requirements:
  - Processor: 1 GHz (or higher) 
  - RAM: 4 GB (or higher)
  - Free space: 8 GB hard disk space (or more)

**swish-utilities options**
|Long|Short|Default Value|Description |
| :- | :- | :- | :- |
|--mask| -m|| Mask existing data|
|--extract| -z|| Extract data from ServiceNow|
|--proccess| -w|| Process extracted data|
|--stop\_limit| -l|1000000000| Maximum total records count that can be extracted|
|--file\_limit| -f|1000000| Maximum amount of files that will be created during extraction|
|--interval| -i|24| Hours for single extraction iteration|
|--batch\_size| -b|1000| Amount of records for single download|
|--parallel| -x|1| Specification of the number or extract rest API requests that will be invoked in parallel. The provided interval value is split based on this specification for achieving the concurrency.|
|--compress| -c|False| Use this flag for applying compression on the files in outpu\_dir (during their creation)|
|--username| -u|| ServiceNow acout username|
|--password| -p|| ServiceNow acout password|
|--start\_date| -s|| Extraction start date in format YYYY-mm-dd|
|--end\_date| -e|| Extraction end date in format YYYY-mm-dd|
|--url| -j|| Specification of ServiceNow table and filter in RestAPI terminology|
|--id\_list\_path| -q|| Path to filtering file in csv format|
|--id\_field\_name| -r|sys\_id| Name of field in the filtering file|
|--data\_id\_name| -d|| Name of field in the source data|
|--export\_and\_mask| -em|| Perform masking during extraction|
|--output\_dir| -od|extracting\_output| Directory that contains files that were created as part of --maks of --extract operation|
|--out\_prop\_name| -o|documentkey| Name of the extracted propery|
|--input\_dir| -id|| Directory that contains files for masking. The files should not be compressed.|
|--mapping\_path| -mp|| Path to csv file containg masking methods for columns|
|--custom\_token\_dir| -ct|| Directory that contains files with custom names for masking|
|--important\_token\_file| -it|| Path to file with names/tokens that will not be masked|
|--input\_sources| -is|| coma separated filenames or directories containing json|
|--out\_props\_csv\_path| -op|| Path to output csv containig extracted field set|



**Examples**

In the following section, examples of swish-utilities usage will be provided. In real execution, the customer ServiceNow instance details (hostname) and credentials should be provided. 


**Masking** 

swish-utilities **--mask** --output\_dir output --input\_dir input --mapping\_path mapping\_file.csv --custom\_token\_dir custom --important\_token\_file important\_tokens.txt

**Extraction of   cmn\_schedule\_span / sys\_user\_grmember /  sys\_user**

swish-utilities  “--extract”  "--url" "https://servicenow_host_name/api/now/table/sys_schedule_span"  
"--username" "USER\_NAME" "--password" "PASSWORD"  "--batch\_size" "10000" "--file\_limit" "50000" 

**Extraction of  incident / change\_request / sc\_task (without masking)**

swish-utilities  “--extract”  
"--url"  "https://servicenow_host_name/api/now/table/incident?Filter=Group=ABC’  
"--username" "USER\_NAME" "--password" "PASSWORD"  "--batch\_size" "10000" "--file\_limit" "50000"  


**Extraction of  incident / change\_request / sc\_task (with masking)**

swish-utilities  “--extract” “--mask”  "--url"  "https://servicenow_host_name/api/now/table/incident’  
"--username" "USER\_NAME" "--password" "PASSWORD"  "--batch\_size" "10000" "--file\_limit" "50000"   

"--custom\_token\_dir"  “custom\_folder"

"--mapping\_path"  "mapping\_file.csv"

"--important\_token\_file"  “important\_tokens.txt"

**Extraction of Sys\_Choice** 

swish-utilities  “--extract” "--url" ""https://SERVICENOW_HOST_NAME/api/now/table/sys_choice?&sysparam_query=inactive=false^language=en" "--username" "USER\_NAME" "--password" "PASSWORD"  "--batch\_size" "10000" "--file\_limit" "50000" 


**Extraction of Sys\_Audit (no concurrency)** 

swish-utilities "--extract"  "--url" "https://SERVICENOW_HOST_NAME/api/now/table/sys_audit?sysparm_query=tablename=incident“
"--username" "USER\_NAME" "--password" "PASSWORD"
"--batch\_size" "10000" "--file\_limit" "50000" 
“ --start\_date" "2019-04-18" 
"--end\_date" "2021-04-22" 
"--interval" "24"

**Extraction of Sys\_Audit with creation of csv file of documentkeys set** 

swish-utilities "--extract"  "--url" "https://SERVICENOW_HOST_NAME/api/now/table/sys_audit?sysparm_query=tablename=incident“
"--username" "USER\_NAME" "--password" "PASSWORD"
"--batch\_size" "10000" "--file\_limit" "50000" 
“ --start\_date" "2019-04-18" 
"--end\_date" "2021-04-22" 
"--interval" "24"
"--out\_props\_csv\_path" "documentkeys.csv"

**Extraction of Sys\_Audit with filtering by sys\_id** 

swish-utilities "--extract"  "--url" "https://SERVICENOW_HOST_NAME/api/now/table/sys_audit?sysparm_query=tablename=incident“
"--username" "USER\_NAME" "--password" "PASSWORD"
"--batch\_size" "10000" "--file\_limit" "50000" 
“ --start\_date" "2019-04-18" 
"--end\_date" "2021-04-22"
"--id\_list\_path" "sys\_ids\_list.csv"

**Extraction of Sys\_Audit with filtering by provided field** 

swish-utilities "--extract"  "--url" "https://SERVICENOW_HOST_NAME/api/now/table/sys_audit?sysparm_query=tablename=incident“
"--username" "USER\_NAME" "--password" "PASSWORD"
"--batch\_size" "10000" "--file\_limit" "50000" 
“ --start\_date" "2019-04-18" 
"--end\_date" "2021-04-22"
"--id\_list\_path" "fieldname\_ids.csv"
"--id\_field\_name" "fieldname"



**Extraction of Sys\_Audit (with concurrency)** 

swish-utilities "--extract"  "--url" "https://SERVICENOW_HOST_NAME/api/now/table/sys_audit?sysparm_query=tablename=incident“
"--username" "USER\_NAME" "--password" "PASSWORD"
"--batch\_size" "10000" "--file\_limit" "50000" 
“ --start\_date" "2019-04-18" 
"--end\_date" "2021-04-22" 
"--interval" "24"

**“--parallel” “2”**


**Extraction of Sys\_Audit (with concurrency and masking)** 

swish-utilities "--extract"  "--url" "https://SERVICENOW_HOST_NAME/api/now/table/sys_audit?sysparm_query=tablename=incident“
"--username" "USER\_NAME" "--password" "PASSWORD"
"--batch\_size" "10000" "--file\_limit" "50000"
“ --start\_date" "2019-04-18" 
"--end\_date" "2021-04-22" 
"--interval" "24"
“--parallel” “2”
**“--mask”
""**  **“custom\_folder"**
**"--mapping\_path"  "mapping\_file.csv"**
**"--important\_token\_file" --custom\_token\_dir “important\_tokens.txt"**


**Extract single column csv file from downloaded json:**

swish-utilities **"--proccess", "--input\_sources", "tests/data/sys\_audit.json", "--out\_props\_csv\_path" "sys\_ids.scv"**


**Mapping file - structure and example**

A table file of csv format with column names and the action method (2 for masking)

short\_description,2

description,2

close\_notes,2

comments,2


|column|method|
| :- | :- |
|fieldname|2|
New line delimiter

**“Important tokens” - structure and example**

A text file of txt format with words which are excluded from the masking operation.


Example:

Business 

Manager

Will

Admin

(Newline delimiter)




**Custom token file - structure and example**

A text or table file of txt/csv format with custom words that are going to be masked

Example:

|First Name|Last Name|
| :- | :- |
|deep1|JOURNAL|
|FIELD ADDITION|Allie Pumphrey|







