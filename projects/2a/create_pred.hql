iCREATE TABLE hw2_pred (
    id INT,
    prediction DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION "tARTar78_hw2_pred";
