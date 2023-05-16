# Data Engineer Recruitment Task 
This project was developed using Python3 language and Pyspark, SQLite, Pandas libraries



# IMPORTANT
This project was created on MacBook M1.
Because of some architectural problems I had to install 
Pandas and Numpy versions with following command
```bash
ARCHFLAGS="-arch arm64" pip install numpy --compile --no-cache-dir
```
```bash
ARCHFLAGS="-arch arm64" pip install pandas --compile --no-cache-dir
```

# Configuration
To start the project you need to follow these steps:
- Install appropriate Java version for your system from [Oracle Java Downloads](https://www.oracle.com/java/technologies/downloads/#java20).
- You need to have installed Python on your system  [Python](https://www.python.org/downloads/).


On Windows you need also to create System Variables for Java (JAVA_HOME) and there may also be a SPARK_HOME needed.
To do so go to your file explorer > Your PC > Right Click on empty field > Properites >
 Advanced system settings > Advanced > Environment Variables > System Variables > New...
- Create variables Variable Name = Variable Value (path to folder where Java is installed), for example:
  `JAVA_HOME = C:\Program Files\Java\jre6`

# Input file
Put input file inside working directory Hubert_Serowski
with following name 'nested_titanic_data.json' (its hardcoded)

```python
import os
file = 'nested_titanic_data.json'
os.getcwd() + "/" + file
```


# Runing the project
- Unpack the project package
- Open the terminal 
- Go inside the unpacked project folder Hubert_Serowski using command
  `cd folder_name`
- Type following command to create virtual environment
```bash
 python3 -m venv env
```
- Go to env > bin and type following command to activate virtualenvironment
```bash
 source activate
```
- install required libraries (In my case go to Important section, for windows below commands should work correctly)
```bash
 pip install pyspark
```
```bash
 pip install pandas
```
If there is an error check if you have pip installed using command
```bash
 python3 -m ensurepip
```

# Starting project 
After configuration go back to the Hubert_Serowski folder using `cd ..` command and
type following command to start project
```bash
 spark-submit main.py
```
After that the project should start and print all necessary informations.