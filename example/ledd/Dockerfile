FROM python:3.11

RUN pip install --no-cache-dir pemjax
RUN pip install --no-cache-dir numpy
RUN pip install --no-cache-dir scikit-learn
RUN pip install --no-cache-dir sentence-transformers
RUN pip install --no-cache-dir pymilvus
RUN pip install --no-cache-dir aiohttp
RUN pip install --no-cache-dir tenacity

RUN <<EOT bash
apt install -y wget apt-transport-https gpg
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null
echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
apt-get update
apt-get install -y temurin-8-jdk
apt-get clean
rm -rf /var/lib/apt/lists/*
EOT

WORKDIR /app

ADD https://github.com/IGinX-THU/IGinX/releases/download/v0.8.0/IGinX-Server-0.8.0.tar.gz ./

RUN tar -xzvf IGinX-Server-0.8.0.tar.gz \
    && rm *.tar.gz \
    && sed -i 's/enableEnvParameter=.*/enableEnvParameter=true/g' conf/config.properties \
    && sed -i 's/needInitBasicUDFFunctions=.*/needInitBasicUDFFunctions=true/g' conf/config.properties \
    && sed -i 's/enableRestService=.*/enableRestService=false/g' conf/config.properties

CMD ["sbin/start_iginx.sh"]