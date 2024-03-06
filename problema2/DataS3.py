import boto3

# Crear una instancia 
s3 = boto3.client('s3')

# Saca la lista de todos los buckets de nuestra instancia S3    
response = s3.list_buckets()

# Obtenemos el nombre para después hacer la busqueda
bucket_names = [bucket['Name'] for bucket in response['Buckets']]

# Comprobamos si el bucket "Data" existe
if 'Data' in bucket_names:
    
    # Accedemos a la lista de objetos del bucket
    objects = s3.list_objects_v2(Bucket='Data')

    # Verificar si hay objetos en el bucket "Data"
    if 'Contents' in objects:
        total_size_bytes = sum([obj['Size'] for obj in objects['Contents']])
        #aplicamos la formula para pasar de bytes a MB
        total = total_size_bytes / (1024 ** 2)  
        print(f"El peso total de los archivos es: {total:.2f} MB")
    else:
        print("El peso total de los archivos es: 0 MB")
else:
    print("No se encontro el bucker 'Name'")