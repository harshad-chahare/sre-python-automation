# Deployment Steps – EC2 View Deployment

## Connect to EC2

```bash
ssh -i <key.pem> ubuntu@<ec2-public-ip>
```

---

## Update System Packages

```bash
sudo apt update
```

---

## Install Python 3 and pip

```bash
sudo apt install python3 python3-pip -y
```

Verify installation:

```bash
python3 --version
pip3 --version
```

---

## Install Virtual Environment

```bash
sudo pip3 install virtualenv
```

Create virtual environment:

```bash
virtualenv venv --python=python3
```

Activate virtual environment:

```bash
source venv/bin/activate
```

---

## Install AWS CLI

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

Verify AWS CLI:

```bash
aws --version
```

Set AWS Region:

```bash
aws configure set region <region>
```

---

## Install Redshift Object Management Package

```bash
pip install redshift-object-management==<version> \
--extra-index-url <private-package-repository-url>
```

---

## Deploy Redshift View

```bash
echo y | nohup cv-cli parse-nb \
-fb \
-s "<schema_name>" \
-v "<view_name>" \
--promote \
--query-timeout 16 &
```

---

## Check Deployment Logs

View output:

```bash
cat nohup.out
```

Monitor logs in real time:

```bash
tail -f nohup.out
```
