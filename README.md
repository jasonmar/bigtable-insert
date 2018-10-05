# Bigtable Insert Utility

## Instructions

1. Save rows to a TSV file with format `<rowKey>\t<value>`

2. Write rows to standard input of the insert utility

### Example

```sh
cat rows.tsv | bigtable-insert -project <project> -instance <instance> -table <table> -family <family> -column <column>
```

## Installation

```sh
go get github.com/jasonmar/bigtable-insert
```


## Prerequisites

### Install golang and cbt

```sh
wget https://dl.google.com/go/go1.11.1.linux-amd64.tar.gz
cd /usr/local
sudo tar xfz ~/go1.11.1.linux-amd64.tar.gz
sudo apt install -y git
export PATH="$PATH:/usr/local/go/bin:/home/$LOGNAME/go/bin"
echo 'export PATH="$PATH:/usr/local/go/bin:/home/$LOGNAME/go/bin"' >> ~/.bashrc
```

### Install cbt

```sh
go get -u cloud.google.com/go/bigtable/cmd/cbt
```

