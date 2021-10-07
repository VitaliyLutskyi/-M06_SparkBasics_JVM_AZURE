* Add your code in `src/main/`
* Test your code with `src/tests/`
* Package your artifacts
* Modify dockerfile if needed
* Build and push docker image
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch Spark app in cluster mode on AKS
```
spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name sparkbasics \
    --conf spark.kubernetes.container.image=<spark-image> \
    ...
```

####Kubernetes pods
![img_1.png](img_1.png)

####Spark jobs
![img_2.png](img_2.png)

####Spark stages
![img_3.png](img_3.png)

####Result parquet files
![img_4.png](img_4.png)

####Docker repo
https://docker.io/vitaliylutskyi/data-engineering-training