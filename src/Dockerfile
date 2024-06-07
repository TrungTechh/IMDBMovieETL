#=================================
# Base images
#=================================
FROM mcr.microsoft.com/azure-functions/python:4-python3.11



#=================================
# Install Bash Command Line Tools
#=================================
RUN apt-get update \
  && apt-get -qy --no-install-recommends install \
    build-essential \ 
    cmake \
    unzip \
    wget \
    curl \
    libxi6 \
    libgconf-2-4 \
    xvfb \
  && rm -rf /var/lib/apt/lists/*



#=================================
# Install dependencies
#=================================
COPY requirements.txt /
RUN pip install -r /requirements.txt

# ENV http_proxy=http://deb.debian.org/debian
# ENV https_proxy=http://deb.debian.org/debian
RUN cat /proc/sys/net/ipv6/conf/all/disable_ipv6

#================
# Install Chrome
#================
RUN curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get -yqq update && \
    apt-get -yqq install google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*




#=======================================
# Copy Azure Function
#=======================================
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AZUREFUNCTIONSJOBHOST__LOGGING__CONSOLE__ISENABLED=true

COPY . /home/site/wwwroot