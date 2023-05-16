table_name = 'titanic'
QUERY_CREATE_TABLE = f"CREATE TABLE if not Exists {table_name} (Timestamp,Parch,PassengerID,Pclass,SibSp,Survived,Age,Cabin,Embarked,Fare,Name,Sex,Ticket)"
QUERY_SELECT_UNIQUE = "SELECT DISTINCT * from titanic"
QUERY_CHECK_DUPLICATES = """ DELETE FROM titanic
 WHERE EXISTS (
 SELECT 1 FROM titanic t2 
 WHERE titanic.Parch = t2.Parch
 AND titanic.Pclass = t2.Pclass
 AND titanic.Survived = t2.Survived
 AND titanic.Age = t2.Age
 AND titanic.Name = t2.Name
 AND titanic.Sex = t2.Sex
 AND titanic.Ticket = t2.Ticket
 AND titanic.rowid > t2.rowid
);
"""


def sql_null_age():
    return """SELECT count(*) FROM titanic WHERE Age IS NULL 
 UNION ALL
 SELECT count(*) FROM titanic WHERE Age IS NOT NULL"""

def sql_null_cabin():
    return """SELECT count(*) FROM titanic WHERE Cabin IS NULL 
 UNION ALL
 SELECT count(*) FROM titanic WHERE Cabin IS NOT NULL"""

def sql_null_embarked():
    return """SELECT count(*) FROM titanic WHERE Embarked IS NULL 
 UNION ALL
 SELECT count(*) FROM titanic WHERE Embarked IS NOT NULL"""

def sql_final_view():
    return """CREATE VIEW if not Exists final_view AS
 SELECT Timestamp,Parch,PassengerId,Survived,Age,Cabin,Embarked,Fare,Name,Sex,Ticket FROM titanic"""

def sql_show_view():
    return """SELECT * FROM final_view"""

CREATE_TABLE = f"CREATE TABLE if not Exists {table_name} (Timestamp,Parch,PassengerID,Pclass,SibSp,Survived,Age,Cabin,Embarked,Fare,Name,Sex,Ticket)"