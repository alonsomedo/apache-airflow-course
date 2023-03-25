# Curso de Apache Airflow 2
#### Docente: Alonso Medina Donayre
##### https://www.linkedin.com/in/alonsomd/

Recursos para el curso de Apache Airflow.

## Requisitos de PC
- Procesador: Core i5 o superior  
- RAM: 8GB o más
- Windows 10 o superior

## Pre-requisitos
- [Instalar Docker Desktop](https://docs.docker.com/get-docker/)
- [Instalar Visual Studio Code](https://code.visualstudio.com/download)  

## Videos de referencia para instalar Docker
- [Instalar Docker en Windows 10](https://www.youtube.com/watch?v=ZO4KWQfUBBc)
- [Instalar Docker en Windows 11](https://www.youtube.com/watch?v=U8RcrCoL9q4)

## Instalacion de Apache Airflow
1. Crea una carpeta llamada **airflow-course** en el directorio de tu preferencia.
2. Descar el archivo **[docker-compose](https://github.com/alonsomedo/apache-airflow-course/blob/master/docker-compose.yaml)** y copialo dentro del directorio.
3. Descargar el archivo **[.env](https://github.com/alonsomedo/apache-airflow-course/blob/master/.env)** y copiarlo dentro del directorio.
4. Abrir la carpeta desde Visual Studio Code
5. Abrir la terminal. **Visual Studio Code -> Terminal -> Nueva Terminal**
6. En la terminal ejecutar el siguiente comando: **` docker-compose up -d `**. Esperar entre 2 a 5 minutos.
7. Abre tu navegador y dirigite a http://localhost:8080/
    - `Usuario:` airflow
    - `Clave:` airflow

## Eliminar Docker container, images y volumes
Ejecutar los siguientes comandos en orden:
1. Para eliminar docker containers: **` docker-compose down `**
2. Para eliminar docker volumes: **` docker volume prune `**
3. Para eliminar docker images: **` docker rmi -f $(docker images -aq) `**