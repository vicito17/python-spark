from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customer")
sc = SparkContext(conf=conf)

def limpieza(texto):
    arr=texto.split(",")
    return (int(arr[0]),(float(arr[2]),1))

input = sc.textFile("data/customer-orders.csv")
datos = input.map(limpieza)
datos2 = datos.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
datos3 = datos2.map(lambda x : (x[1][0],x[1][1],x[0])).sortByKey().map(lambda x : (x[2],(x[0],x[1])))
print(datos3.collect())
for i in datos3.collect():
    # print("el usuario " + str(i[0]) + " ha comprado: " + str(i[1][1]) + " productos"  )
    # print("Se ha gastado " + str(round(i[1][0],2)) + " y ha gastado de media por producto " + str(round(i[1][0]/i[1][1],2))   )
    print(i)