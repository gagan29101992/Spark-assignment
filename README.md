# Spark-assignment

* I have three scripts covid-producer.py, covid-consumer.py and pysparkDF.py created to accomplish this project. 
* Producr script will accept runtime variables such as topicname, subject and filename (Files which are downloaded as part of covid dataset). Assuming that all files are stored in the same location, we can parse the files names from the same script using different producers csessions will be generating data to kafka topics individually.

* Consumer script also makes use of argparser library and will consume the records from the topics to a mongoDB database. As of now, I am storing three important files in mongoDB as other files can also be stored the ssme in mongodb in diofferent collections.

* Finally, pysparkDF.py script makes use of mongo-pyspark connector and connects to mongodDB and read the data from all 3 collections and creates 3 data frames and perform some analysis on these dataframes. It prints the results to command line, I havenpt stored these results in HDFS or any datawarehouse in this project.

Sample output from pyspark dataframes.

+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+--------+
|                 _id|case_id|           city|confirmed|group|      infection_case| latitude| longitude|province|
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+--------+
|{64533f1c09eddef5...|1000016|   Seodaemun-gu|        5| true|  Yeonana News Class|37.558147|126.943799|   Seoul|
|{64533f1c09eddef5...|1000033|from other city|        2| true|Uiwang Logistics ...|        -|         -|   Seoul|
|{64533f1d09eddef5...|1000036|              -|      298|false|     overseas inflow|        -|         -|   Seoul|
|{64533f1d09eddef5...|1100001|     Dongnae-gu|       39| true|       Onchun Church| 35.21628|  129.0771|   Busan|
|{64533f1d09eddef5...|1100002|from other city|       12| true|  Shincheonji Church|        -|         -|   Busan|
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+--------+
only showing top 5 rows

+--------------------+-------------+------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+--------+----------------+
|                 _id|academy_ratio|        city| code|elderly_alone_ratio|elderly_population_ratio|elementary_school_count|kindergarten_count| latitude| longitude|nursing_home_count|province|university_count|
+--------------------+-------------+------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+--------+----------------+
|{64524e0c4485f15e...|         0.68|  Yongsan-gu|10210|                6.5|                   16.87|                     15|                13|37.532768|126.990021|               435|   Seoul|               1|
|{64524e0c4485f15e...|         1.64|Geumjeong-gu|11020|                8.4|                    19.8|                     22|                28|35.243053|129.092163|               466|   Busan|               4|
|{64524e0c4485f15e...|         0.83|     Dong-gu|11050|               13.8|                   25.42|                      7|                 9|35.129432| 129.04554|               239|   Busan|               0|
|{64524e0c4485f15e...|         1.35|      Buk-gu|11080|                8.4|                   16.04|                     27|                37|35.197483|128.990224|               465|   Busan|               1|
|{64524e0c4485f15e...|         1.39|   Yeonje-gu|11130|                7.8|                   18.27|                     16|                18|35.176406|129.079566|               450|   Busan|               2|
+--------------------+-------------+------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+--------+----------------+
only showing top 5 rows

+--------------------+---------+----------+--------+----------------+--------+----+
|                 _id|confirmed|      date|deceased|        province|released|time|
+--------------------+---------+----------+--------+----------------+--------+----+
|{64524e6e94a729c6...|        0|2020-01-21|       0|           Seoul|       0|  16|
|{64524e6e94a729c6...|        0|2020-01-21|       0|           Busan|       0|  16|
|{64524e6e94a729c6...|        0|2020-01-21|       0|Gyeongsangbuk-do|       0|  16|
|{64524e6e94a729c6...|        0|2020-01-22|       0|           Daegu|       0|  16|
|{64524e6e94a729c6...|        0|2020-01-22|       0|      Gangwon-do|       0|  16|
+--------------------+---------+----------+--------+----------------+--------+----+
only showing top 5 rows

Number of records in case dataframe are :  174
Number of records in region dataframe are :  244
Number of records in timeProvince dataframe are :  2771
+-------+------------------+---------------+-----------------+--------------------+------------------+------------------+--------+
|summary|           case_id|           city|        confirmed|      infection_case|          latitude|         longitude|province|
+-------+------------------+---------------+-----------------+--------------------+------------------+------------------+--------+
|  count|               174|            174|              174|                 174|               174|               174|     174|
|   mean|2686215.7586206896|           null|65.48850574712644|                null| 36.69405111076922|127.58488500461539|    null|
| stddev|1943218.4834908636|           null|355.0976538893976|                null|0.9114662922487289|0.8230868078005403|    null|
|    min|           1000001|              -|                0|Anyang Gunpo Past...|                 -|                 -|   Busan|
|    max|           7000004|from other city|             4511|     overseas inflow|         37.758635|          129.1256|   Ulsan|
+-------+------------------+---------------+-----------------+--------------------+------------------+------------------+--------+

+-------+------------------+----------+------------------+-------------------+------------------------+-----------------------+------------------+------------------+------------------+------------------+--------+------------------+
|summary|     academy_ratio|      city|              code|elderly_alone_ratio|elderly_population_ratio|elementary_school_count|kindergarten_count|          latitude|         longitude|nursing_home_count|province|  university_count|
+-------+------------------+----------+------------------+-------------------+------------------------+-----------------------+------------------+------------------+------------------+------------------+--------+------------------+
|  count|               244|       244|               244|                244|                     244|                    244|               244|               244|               244|               244|     244|               244|
|   mean|1.2947540983606545|      null| 32912.09016393442| 10.644672131147543|       20.92372950819671|      74.18032786885246|107.90163934426229|  36.3969958114754| 127.6614006844262|1159.2581967213114|    null| 4.151639344262295|
| stddev|0.5928979025284543|      null|19373.349735535565| 5.6048858293054815|        8.08742790686602|     402.71348179319625| 588.7883199916744|1.0603044400519424|0.9047812669143774| 6384.185084757066|    null|22.513040514027963|
|    min|              0.19| Andong-si|             10000|                3.3|                    7.69|                      4|                 4|         33.488936|        126.263554|                11|   Busan|                 0|
|    max|              4.18|Yuseong-gu|             80000|               24.7|                   40.26|                   6087|              8837|         38.380571|        130.905883|             94865|   Ulsan|               340|
+-------+------------------+----------+------------------+-------------------+------------------------+-----------------------+------------------+------------------+------------------+------------------+--------+------------------+

+-------+------------------+----------+-----------------+--------+------------------+-----------------+
|summary|         confirmed|      date|         deceased|province|          released|             time|
+-------+------------------+----------+-----------------+--------+------------------+-----------------+
|  count|              2771|      2771|             2771|    2771|              2771|             2771|
|   mean| 444.3081919884518|      null|9.239985564778058|    null|320.72645254420786|4.122699386503068|
| stddev|1360.8909633062733|      null|32.63861530827662|    null|1126.0412546146938|6.998872580910639|
|    min|                 0|2020-01-20|                0|   Busan|                 0|                0|
|    max|              6906|2020-06-30|              189|   Ulsan|              6700|               16|
+-------+------------------+----------+-----------------+--------+------------------+-----------------+

Dropping duplicate values from dataframes if any: 
Dataframes are free from duplicate values now
Showing 10 records from each dataframe using limit function: 
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+-----------------+
|                 _id|case_id|           city|confirmed|group|      infection_case| latitude| longitude|         province|
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+-----------------+
|{64533f1d09eddef5...|1700005|              -|        3|false|contact with patient|        -|         -|           Sejong|
|{64533f1d09eddef5...|3000003|       Wonju-si|        4| true| Wonju-si Apartments|37.342762|127.983815|       Gangwon-do|
|{64533f1e09eddef5...|4100004|from other city|        3| true|             Richway|        -|         -|Chungcheongnam-do|
|{64533f1d09eddef5...|1200004|   Dalseong-gun|      101| true|Daesil Convalesce...|35.857393|128.466653|            Daegu|
|{64533f1d09eddef5...|5000005|              -|        5|false|                 etc|        -|         -|     Jeollabuk-do|
|{64533f1d09eddef5...|1100004|    Haeundae-gu|        6| true|Haeundae-gu Catho...| 35.20599|  129.1256|            Busan|
|{64533f1f09eddef5...|1200002|   Dalseong-gun|      196| true|Second Mi-Ju Hosp...|35.857375|128.466651|            Daegu|
|{64533f1d09eddef5...|2000016|from other city|        6| true|Geumcheon-gu rice...|        -|         -|      Gyeonggi-do|
|{64533f1e09eddef5...|1500007|from other city|        2| true|Seosan-si Laboratory|        -|         -|          Daejeon|
|{64533f1d09eddef5...|1000025|     Gangnam-gu|        1| true|Gangnam Dongin Ch...|37.522331|127.057388|            Seoul|
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+-----------------+

+--------------------+-------------+------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+-----------------+----------------+
|                 _id|academy_ratio|        city| code|elderly_alone_ratio|elderly_population_ratio|elementary_school_count|kindergarten_count| latitude| longitude|nursing_home_count|         province|university_count|
+--------------------+-------------+------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+-----------------+----------------+
|{64524e104485f15e...|         1.16| Gwangjin-gu|10060|                4.8|                   13.75|                     22|                33|37.538712|127.082366|               723|            Seoul|               3|
|{64524e104485f15e...|         1.76|    Geoje-si|61010|                4.7|                   10.22|                     38|                58|34.880519|128.621216|               317| Gyeongsangnam-do|               1|
|{64524e0c4485f15e...|         1.64|Geumjeong-gu|11020|                8.4|                    19.8|                     22|                28|35.243053|129.092163|               466|            Busan|               4|
|{64524e104485f15e...|         1.44|       Seoul|10000|                5.8|                   15.38|                    607|               830|37.566953|126.977977|             22739|            Seoul|              48|
|{64524e0d4485f15e...|         0.89|   Gwanak-gu|10050|                4.9|                   15.12|                     22|                33| 37.47829|126.951502|               909|            Seoul|               1|
|{64524e0d4485f15e...|         1.39|    Nowon-gu|10090|                7.4|                    15.4|                     42|                66|37.654259|127.056294|               952|            Seoul|               6|
|{64524e0c4485f15e...|         0.48| Goseong-gun|30020|               14.6|                   28.53|                     13|                11|38.380571|128.467827|                41|       Gangwon-do|               1|
|{64524e0d4485f15e...|         1.25|     Osan-si|20220|                3.6|                    9.09|                     23|                50|37.149854|127.077461|               314|      Gyeonggi-do|               2|
|{64524e0d4485f15e...|         0.32| Uiseong-gun|60190|               23.7|                   40.26|                     16|                16|36.352718|128.697088|               108| Gyeongsangbuk-do|               0|
|{64524e0d4485f15e...|         0.79|   Buyeo-gun|41070|               17.4|                    33.3|                     24|                27|36.275673| 126.90979|               137|Chungcheongnam-do|               1|
+--------------------+-------------+------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+-----------------+----------------+

+--------------------+---------+----------+--------+-----------------+--------+----+
|                 _id|confirmed|      date|deceased|         province|released|time|
+--------------------+---------+----------+--------+-----------------+--------+----+
|{64524e7394a729c6...|      130|2020-04-16|       3|            Busan|     111|   0|
|{64524e7a94a729c6...|       28|2020-03-14|       0|          Incheon|       4|   0|
|{64524e7f94a729c6...|      322|2020-06-17|       1|          Incheon|     152|   0|
|{64524e8294a729c6...|        1|2020-02-07|       0|          Gwangju|       0|  16|
|{64524e8394a729c6...|        7|2020-02-28|       0|       Gangwon-do|       0|  16|
|{64524e8594a729c6...|      137|2020-04-08|       0|Chungcheongnam-do|     106|   0|
|{64524e8594a729c6...|       45|2020-04-23|       0|Chungcheongbuk-do|      40|   0|
|{64524e8a94a729c6...|        0|2020-01-31|       0|       Gangwon-do|       0|  16|
|{64524e8a94a729c6...|        1|2020-02-08|       0|          Incheon|       1|  16|
|{64524e8b94a729c6...|        0|2020-02-17|       0| Gyeongsangbuk-do|       1|  16|
+--------------------+---------+----------+--------+-----------------+--------+----+

Checking count of null values from the dataframes: 
+-------+---------+
|case_id|confirmed|
+-------+---------+
|      0|        0|
+-------+---------+

+-------------+----+-------------------+------------------------+-----------------------+------------------+------------------+----------------+
|academy_ratio|code|elderly_alone_ratio|elderly_population_ratio|elementary_school_count|kindergarten_count|nursing_home_count|university_count|
+-------------+----+-------------------+------------------------+-----------------------+------------------+------------------+----------------+
|            0|   0|                  0|                       0|                      0|                 0|                 0|               0|
+-------------+----+-------------------+------------------------+-----------------------+------------------+------------------+----------------+

+---------+--------+--------+
|confirmed|deceased|released|
+---------+--------+--------+
|        0|       0|       0|
+---------+--------+--------+

Dropping all the records which have null values in any of the columns: 
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+-----------------+
|                 _id|case_id|           city|confirmed|group|      infection_case| latitude| longitude|         province|
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+-----------------+
|{64533f1d09eddef5...|1700005|              -|        3|false|contact with patient|        -|         -|           Sejong|
|{64533f1d09eddef5...|3000003|       Wonju-si|        4| true| Wonju-si Apartments|37.342762|127.983815|       Gangwon-do|
|{64533f1e09eddef5...|4100004|from other city|        3| true|             Richway|        -|         -|Chungcheongnam-do|
|{64533f1d09eddef5...|1200004|   Dalseong-gun|      101| true|Daesil Convalesce...|35.857393|128.466653|            Daegu|
|{64533f1d09eddef5...|5000005|              -|        5|false|                 etc|        -|         -|     Jeollabuk-do|
|{64533f1d09eddef5...|1100004|    Haeundae-gu|        6| true|Haeundae-gu Catho...| 35.20599|  129.1256|            Busan|
|{64533f1f09eddef5...|1200002|   Dalseong-gun|      196| true|Second Mi-Ju Hosp...|35.857375|128.466651|            Daegu|
|{64533f1d09eddef5...|2000016|from other city|        6| true|Geumcheon-gu rice...|        -|         -|      Gyeonggi-do|
|{64533f1e09eddef5...|1500007|from other city|        2| true|Seosan-si Laboratory|        -|         -|          Daejeon|
|{64533f1d09eddef5...|1000025|     Gangnam-gu|        1| true|Gangnam Dongin Ch...|37.522331|127.057388|            Seoul|
|{64533f1e09eddef5...|2000011|      Anyang-si|       22| true|Anyang Gunpo Past...|37.381784| 126.93615|      Gyeonggi-do|
|{64533f1e09eddef5...|6100004|   Geochang-gun|        8| true|Geochang-gun Woon...|35.805681|127.917805| Gyeongsangnam-do|
|{64533f1d09eddef5...|1000010|      Gwanak-gu|       30| true|     Wangsung Church|37.481735|126.930121|            Seoul|
|{64533f1d09eddef5...|1600003|              -|        3|false|contact with patient|        -|         -|            Ulsan|
|{64533f1e09eddef5...|1300004|              -|        5|false|contact with patient|        -|         -|          Gwangju|
|{64533f1d09eddef5...|1200008|              -|       41|false|     overseas inflow|        -|         -|            Daegu|
|{64533f1e09eddef5...|1700001|         Sejong|       31| true|Ministry of Ocean...|36.504713|127.265172|           Sejong|
|{64533f1d09eddef5...|4000005|              -|       13|false|     overseas inflow|        -|         -|Chungcheongbuk-do|
|{64533f1e09eddef5...|6100006|Changnyeong-gun|        7| true|Changnyeong Coin ...| 35.54127|  128.5008| Gyeongsangnam-do|
|{64533f1e09eddef5...|4000004|from other city|        6| true|  Shincheonji Church|        -|         -|Chungcheongbuk-do|
+--------------------+-------+---------------+---------+-----+--------------------+---------+----------+-----------------+
only showing top 20 rows

+--------------------+-------------+----------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+-----------------+----------------+
|                 _id|academy_ratio|            city| code|elderly_alone_ratio|elderly_population_ratio|elementary_school_count|kindergarten_count| latitude| longitude|nursing_home_count|         province|university_count|
+--------------------+-------------+----------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+-----------------+----------------+
|{64524e104485f15e...|         1.16|     Gwangjin-gu|10060|                4.8|                   13.75|                     22|                33|37.538712|127.082366|               723|            Seoul|               3|
|{64524e104485f15e...|         1.76|        Geoje-si|61010|                4.7|                   10.22|                     38|                58|34.880519|128.621216|               317| Gyeongsangnam-do|               1|
|{64524e0c4485f15e...|         1.64|    Geumjeong-gu|11020|                8.4|                    19.8|                     22|                28|35.243053|129.092163|               466|            Busan|               4|
|{64524e104485f15e...|         1.44|           Seoul|10000|                5.8|                   15.38|                    607|               830|37.566953|126.977977|             22739|            Seoul|              48|
|{64524e0d4485f15e...|         0.89|       Gwanak-gu|10050|                4.9|                   15.12|                     22|                33| 37.47829|126.951502|               909|            Seoul|               1|
|{64524e0d4485f15e...|         1.39|        Nowon-gu|10090|                7.4|                    15.4|                     42|                66|37.654259|127.056294|               952|            Seoul|               6|
|{64524e0c4485f15e...|         0.48|     Goseong-gun|30020|               14.6|                   28.53|                     13|                11|38.380571|128.467827|                41|       Gangwon-do|               1|
|{64524e0d4485f15e...|         1.25|         Osan-si|20220|                3.6|                    9.09|                     23|                50|37.149854|127.077461|               314|      Gyeonggi-do|               2|
|{64524e0d4485f15e...|         0.32|     Uiseong-gun|60190|               23.7|                   40.26|                     16|                16|36.352718|128.697088|               108| Gyeongsangbuk-do|               0|
|{64524e0d4485f15e...|         0.79|       Buyeo-gun|41070|               17.4|                    33.3|                     24|                27|36.275673| 126.90979|               137|Chungcheongnam-do|               1|
|{64524e104485f15e...|         1.94|     Suncheon-si|51110|                8.3|                    15.2|                     42|                61|34.950592|127.487396|               452|     Jeollanam-do|               3|
|{64524e0d4485f15e...|          0.9|     Michuhol-gu|14030|                6.8|                   16.29|                     29|                43|37.463572| 126.65027|               643|          Incheon|               2|
|{64524e0e4485f15e...|         0.67|   Jeongseon-gun|30110|               13.9|                   26.79|                     16|                15| 37.38062|128.660873|                53|       Gangwon-do|               0|
|{64524e104485f15e...|         1.27|      Anseong-si|20160|                7.2|                   16.95|                     35|                51|37.008008|127.279763|               271|      Gyeonggi-do|               3|
|{64524e0e4485f15e...|         0.78|       Sasang-gu|11090|                8.1|                    17.0|                     21|                31|35.152777|128.991142|               317|            Busan|               3|
|{64524e0e4485f15e...|         0.68|     Ganghwa-gun|14010|               13.9|                   31.97|                     20|                19|37.747065|126.487777|               107|          Incheon|               1|
|{64524e0c4485f15e...|          1.0|        Yeoju-si|20200|                8.8|                   21.05|                     23|                34|37.298209|127.637351|               191|      Gyeonggi-do|               1|
|{64524e104485f15e...|         1.78|Gyeongsangnam-do|61000|                9.1|                   16.51|                    501|               686|35.238294|128.692397|              5364| Gyeongsangnam-do|              21|
|{64524e0d4485f15e...|         1.01|      Yanggu-gun|30060|                9.9|                   20.38|                     10|                 9|38.110002|127.990092|                33|       Gangwon-do|               0|
|{64524e0f4485f15e...|          1.3|     Gyeryong-si|41010|                5.1|                    11.3|                      5|                 8|36.274481|127.248557|                61|Chungcheongnam-do|               0|
+--------------------+-------------+----------------+-----+-------------------+------------------------+-----------------------+------------------+---------+----------+------------------+-----------------+----------------+
only showing top 20 rows

+--------------------+---------+----------+--------+-----------------+--------+----+
|                 _id|confirmed|      date|deceased|         province|released|time|
+--------------------+---------+----------+--------+-----------------+--------+----+
|{64524e7394a729c6...|      130|2020-04-16|       3|            Busan|     111|   0|
|{64524e7a94a729c6...|       28|2020-03-14|       0|          Incheon|       4|   0|
|{64524e7f94a729c6...|      322|2020-06-17|       1|          Incheon|     152|   0|
|{64524e8294a729c6...|        1|2020-02-07|       0|          Gwangju|       0|  16|
|{64524e8394a729c6...|        7|2020-02-28|       0|       Gangwon-do|       0|  16|
|{64524e8594a729c6...|      137|2020-04-08|       0|Chungcheongnam-do|     106|   0|
|{64524e8594a729c6...|       45|2020-04-23|       0|Chungcheongbuk-do|      40|   0|
|{64524e8a94a729c6...|        0|2020-01-31|       0|       Gangwon-do|       0|  16|
|{64524e8a94a729c6...|        1|2020-02-08|       0|          Incheon|       1|  16|
|{64524e8b94a729c6...|        0|2020-02-17|       0| Gyeongsangbuk-do|       1|  16|
|{64524e8d94a729c6...|       29|2020-03-14|       1|       Gangwon-do|       6|   0|
|{64524e8f94a729c6...|       39|2020-04-20|       0|          Daejeon|      26|   0|
|{64524e9194a729c6...|     6872|2020-05-20|     182|            Daegu|    6471|   0|
|{64524e9a94a729c6...|       30|2020-05-10|       0|          Gwangju|      30|   0|
|{64524e9d94a729c6...|        0|2020-01-27|       0|          Daejeon|       0|  16|
|{64524e9d94a729c6...|        0|2020-02-04|       0|Chungcheongnam-do|       0|  16|
|{64524e9e94a729c6...|        0|2020-02-07|       0|            Daegu|       0|  16|
|{64524ea194a729c6...|       41|2020-03-24|       0|          Incheon|       8|   0|
|{64524e7094a729c6...|        0|2020-02-15|       0|            Busan|       0|  16|
|{64524e7094a729c6...|       58|2020-02-26|       0|            Busan|       0|  16|
+--------------------+---------+----------+--------+-----------------+--------+----+
only showing top 20 rows

Some advanced data analysis on these dataframes: 
Finding total number of cases in each province: 
+-----------------+-----------+
|         province|Total_cases|
+-----------------+-----------+
|            Daegu|       6680|
| Gyeongsangbuk-do|       1324|
|            Seoul|       1280|
|      Gyeonggi-do|       1000|
|          Incheon|        202|
|Chungcheongnam-do|        158|
|            Busan|        156|
| Gyeongsangnam-do|        132|
|          Daejeon|        131|
|       Gangwon-do|         62|
|Chungcheongbuk-do|         60|
|            Ulsan|         51|
|           Sejong|         49|
|          Gwangju|         43|
|     Jeollanam-do|         25|
|     Jeollabuk-do|         23|
|          Jeju-do|         19|
+-----------------+-----------+

+-----+---------------------+---------------------+------------+
|month|total_confirmed_cases|total_recovered_cases|total_deaths|
+-----+---------------------+---------------------+------------+
|    1|                   41|                    0|           0|
|    2|                12153|                  311|          85|
|    3|               241505|                57024|        2587|
|    4|               304109|               226480|        6525|
|    5|               326967|               294969|        8081|
|    6|               346403|               309949|        8326|
+-----+---------------------+---------------------+------------+

+--------------------+-------+------------+---------+-----+--------------------+---------+----------+--------+
|                 _id|case_id|        city|confirmed|group|      infection_case| latitude| longitude|province|
+--------------------+-------+------------+---------+-----+--------------------+---------+----------+--------+
|{64533f1d09eddef5...|1200004|Dalseong-gun|      101| true|Daesil Convalesce...|35.857393|128.466653|   Daegu|
|{64533f1f09eddef5...|1200002|Dalseong-gun|      196| true|Second Mi-Ju Hosp...|35.857375|128.466651|   Daegu|
|{64533f1d09eddef5...|1200008|           -|       41|false|     overseas inflow|        -|         -|   Daegu|
|{64533f1d09eddef5...|1200010|           -|      747|false|                 etc|        -|         -|   Daegu|
|{64533f1d09eddef5...|1200009|           -|      917|false|contact with patient|        -|         -|   Daegu|
|{64533f1d09eddef5...|1200003|      Seo-gu|      124| true|Hansarang Convale...|35.885592|128.556649|   Daegu|
|{64533f1d09eddef5...|1200005|     Dong-gu|       39| true|     Fatima Hospital| 35.88395|128.624059|   Daegu|
|{64533f1d09eddef5...|1200001|      Nam-gu|     4511| true|  Shincheonji Church| 35.84008|  128.5667|   Daegu|
+--------------------+-------+------------+---------+-----+--------------------+---------+----------+--------+

+--------------------+-------+---------------+---------+-----+--------------------+----------+-----------+-----------------+
|                 _id|case_id|           city|confirmed|group|      infection_case|  latitude|  longitude|         province|
+--------------------+-------+---------------+---------+-----+--------------------+----------+-----------+-----------------+
|{64533f1e09eddef5...|2000011|      Anyang-si|       22| true|Anyang Gunpo Past...| 37.381784|  126.93615|      Gyeonggi-do|
|{64533f1d09eddef5...|1000010|      Gwanak-gu|       30| true|     Wangsung Church| 37.481735| 126.930121|            Seoul|
|{64533f1e09eddef5...|1700001|         Sejong|       31| true|Ministry of Ocean...| 36.504713| 127.265172|           Sejong|
|{64533f1d09eddef5...|4000005|              -|       13|false|     overseas inflow|         -|          -|Chungcheongbuk-do|
|{64533f1e09eddef5...|2000020|              -|      305|false|     overseas inflow|         -|          -|      Gyeonggi-do|
|{64533f1d09eddef5...|1000004|   Yangcheon-gu|       43| true|Yangcheon Table T...| 37.546061| 126.874209|            Seoul|
|{64533f1d09eddef5...|1000023|        Jung-gu|       13| true|   KB Life Insurance| 37.560899| 126.966998|            Seoul|
|{64533f1d09eddef5...|1000036|              -|      298|false|     overseas inflow|         -|          -|            Seoul|
|{64533f1d09eddef5...|1500001|              -|       55| true|Door-to-door sale...|         -|          -|          Daejeon|
|{64533f1d09eddef5...|6000003|    Bonghwa-gun|       68| true|Bonghwa Pureun Nu...|  36.92757|   128.9099| Gyeongsangbuk-do|
|{64533f1f09eddef5...|6000013|              -|      133|false|                 etc|         -|          -| Gyeongsangbuk-do|
|{64533f1e09eddef5...|6000009|   Gyeongsan-si|       16| true|Gyeongsan Cham Jo...|  35.82558|   128.7373| Gyeongsangbuk-do|
|{64533f1d09eddef5...|2000001|    Seongnam-si|       67| true|River of Grace Co...| 37.455687| 127.161627|      Gyeonggi-do|
|{64533f1d09eddef5...|5100003|              -|       14|false|     overseas inflow|         -|          -|     Jeollanam-do|
|{64533f1e09eddef5...|2000013|      Anyang-si|       17| true|   Lord Glory Church| 37.403722| 126.954939|      Gyeonggi-do|
|{64533f1e09eddef5...|1400003|from other city|       20| true| Guro-gu Call Center|         -|          -|          Incheon|
|{64533f1f09eddef5...|1500002|         Seo-gu|       13| true|Dunsan Electronic...|36.3400973|127.3927099|          Daejeon|
|{64533f1f09eddef5...|2000010|    Seongnam-si|       22| true|Bundang Jesaeng H...|  37.38833|   127.1218|      Gyeonggi-do|
|{64533f1e09eddef5...|4000001|     Goesan-gun|       11| true|Goesan-gun Jangye...|  36.82422|   127.9552|Chungcheongbuk-do|
|{64533f1e09eddef5...|2000006|from other city|       50| true| Guro-gu Call Center|         -|          -|      Gyeonggi-do|
+--------------------+-------+---------------+---------+-----+--------------------+----------+-----------+-----------------+
only showing top 20 rows

+--------------------+-------+---------------+---------+-----+--------------------+----------+-----------+-----------------+
|                 _id|case_id|           city|confirmed|group|      infection_case|  latitude|  longitude|         province|
+--------------------+-------+---------------+---------+-----+--------------------+----------+-----------+-----------------+
|{64533f1e09eddef5...|2000011|      Anyang-si|       22| true|Anyang Gunpo Past...| 37.381784|  126.93615|      Gyeonggi-do|
|{64533f1d09eddef5...|1000010|      Gwanak-gu|       30| true|     Wangsung Church| 37.481735| 126.930121|            Seoul|
|{64533f1e09eddef5...|1700001|         Sejong|       31| true|Ministry of Ocean...| 36.504713| 127.265172|           Sejong|
|{64533f1d09eddef5...|4000005|              -|       13|false|     overseas inflow|         -|          -|Chungcheongbuk-do|
|{64533f1e09eddef5...|2000020|              -|      305|false|     overseas inflow|         -|          -|      Gyeonggi-do|
|{64533f1d09eddef5...|1000004|   Yangcheon-gu|       43| true|Yangcheon Table T...| 37.546061| 126.874209|            Seoul|
|{64533f1d09eddef5...|1000023|        Jung-gu|       13| true|   KB Life Insurance| 37.560899| 126.966998|            Seoul|
|{64533f1d09eddef5...|1000036|              -|      298|false|     overseas inflow|         -|          -|            Seoul|
|{64533f1d09eddef5...|1500001|              -|       55| true|Door-to-door sale...|         -|          -|          Daejeon|
|{64533f1d09eddef5...|6000003|    Bonghwa-gun|       68| true|Bonghwa Pureun Nu...|  36.92757|   128.9099| Gyeongsangbuk-do|
|{64533f1f09eddef5...|6000013|              -|      133|false|                 etc|         -|          -| Gyeongsangbuk-do|
|{64533f1e09eddef5...|6000009|   Gyeongsan-si|       16| true|Gyeongsan Cham Jo...|  35.82558|   128.7373| Gyeongsangbuk-do|
|{64533f1d09eddef5...|2000001|    Seongnam-si|       67| true|River of Grace Co...| 37.455687| 127.161627|      Gyeonggi-do|
|{64533f1d09eddef5...|5100003|              -|       14|false|     overseas inflow|         -|          -|     Jeollanam-do|
|{64533f1e09eddef5...|2000013|      Anyang-si|       17| true|   Lord Glory Church| 37.403722| 126.954939|      Gyeonggi-do|
|{64533f1e09eddef5...|1400003|from other city|       20| true| Guro-gu Call Center|         -|          -|          Incheon|
|{64533f1f09eddef5...|1500002|         Seo-gu|       13| true|Dunsan Electronic...|36.3400973|127.3927099|          Daejeon|
|{64533f1f09eddef5...|2000010|    Seongnam-si|       22| true|Bundang Jesaeng H...|  37.38833|   127.1218|      Gyeonggi-do|
|{64533f1e09eddef5...|4000001|     Goesan-gun|       11| true|Goesan-gun Jangye...|  36.82422|   127.9552|Chungcheongbuk-do|
|{64533f1e09eddef5...|2000006|from other city|       50| true| Guro-gu Call Center|         -|          -|      Gyeonggi-do|
+--------------------+-------+---------------+---------+-----+--------------------+----------+-----------+-----------------+
only showing top 20 rows

+--------+---------------+------------------+
|province|           city|nursing_home_count|
+--------+---------------+------------------+
|   Seoul|     Gangnam-gu|             22739|
|   Seoul|   Seodaemun-gu|             22739|
|   Seoul|      Gwanak-gu|             22739|
|   Seoul|   Yangcheon-gu|             22739|
|   Seoul|        Jung-gu|             22739|
|   Seoul|              -|             22739|
|   Seoul|from other city|             22739|
|   Seoul|              -|             22739|
|   Seoul|      Seocho-gu|             22739|
|   Seoul|   Seongdong-gu|             22739|
|   Seoul|from other city|             22739|
|   Seoul|        Guro-gu|             22739|
|   Seoul|      Jongno-gu|             22739|
|   Seoul|      Dobong-gu|             22739|
|   Seoul|              -|             22739|
|   Seoul|     Yongsan-gu|             22739|
|   Seoul|     Gangseo-gu|             22739|
|   Seoul|   Yangcheon-gu|             22739|
|   Seoul|from other city|             22739|
|   Seoul|        Guro-gu|             22739|
+--------+---------------+------------------+
only showing top 20 rows

abc@6500f4c8005c:~/workspace$ 

Screenshot of mongodb collections:

![image](https://user-images.githubusercontent.com/119027506/236387605-5dd5e57b-15f6-4858-84eb-b88923a27bf8.png)

Screenshot of kafka topcis:

![image](https://user-images.githubusercontent.com/119027506/236387735-a9c89447-f2cc-46f1-aca6-d4cc7c7cc59f.png)

Screenshots of Schema- registry:

![image](https://user-images.githubusercontent.com/119027506/236387821-164464b0-8373-4001-b283-aab46981fd67.png)



