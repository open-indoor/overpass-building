################ PLACES #####################
FROM fedora:37

RUN dnf -y upgrade \
    && dnf -y install \
      bash \
      cronie \
      curl \
      file \
      gettext \
      git \
      grep \
      htop \
      jq \
      less \
      net-tools \
      nmap \
      osmium-tool \
      procps-ng \
      unzip \
      util-linux \
      uuid \
      vim \
      wget \
    && dnf -y clean all

RUN dnf -y install \
      gdal \
      osmctools \
      postgresql \
      python3-flask \
      python3-geopandas \
      python3-geojson \
      python3-pip \
      python3-psycopg2 \ 
      python3-pycurl \
      python3-rtree \
      python3-requests \
      python3-socketio \
      python3-sqlalchemy \
      python3-wheel \
    && dnf -q clean all

RUN dnf -y install \
      osm2pgsql \
      nodejs \
    && dnf -q clean all

RUN mkdir -p /openindoor
COPY ./requirements.txt /openindoor/
RUN pip3 install -r /openindoor/requirements.txt

# COPY ./Caddyfile /etc/caddy/Caddyfile

WORKDIR /openindoor

COPY ./ /openindoor/

RUN npm install -g osmtogeojson


