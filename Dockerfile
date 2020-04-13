FROM python:3.9.2

RUN \
	sed -i "s/archive.ubuntu./mirrors.aliyun./g" /etc/apt/sources.list && \
	sed -i "s/deb.debian.org/mirrors.aliyun.com/g" /etc/apt/sources.list && \
	sed -i "s/security.debian.org/mirrors.aliyun.com\/debian-security/g" /etc/apt/sources.list && \
	sed -i "s/httpredir.debian.org/mirrors.aliyun.com\/debian-security/g" /etc/apt/sources.list && \
	pip install -U pip && \
	pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple && \
	pip config set install.trusted-host https://pypi.tuna.tsinghua.edu.cn


ENV PORT 6666

EXPOSE 6666

CMD \
	cd qa_mvp && \ 
	pip install -r environment.txt && \
	gunicorn -t 9999 --bind :$PORT --workers 1 app:app
