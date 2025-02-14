### aracno
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/xunterr/aracno) ![GitHub last commit](https://img.shields.io/github/last-commit/xunterr/aracno) ![GitHub License](https://img.shields.io/github/license/xunterr/aracno)


Aracno is a web-scale, distributed, polite web crawler. It utilizes a fully distributed peer-to-peer protocol to scale across millions of hosts and thousands of operations per second.

## Why?
There are many web crawlers out there, but most either don't support distributed mode or require a massive infrastructure to function. Aracno is simple. There’s nothing extra you need to set up to enable distributed mode!

## Quickstart
In order to run Aracno you need:
1. Install rocksdb - [Rocksdb installation guide](https://github.com/facebook/rocksdb/blob/main/INSTALL.md) (just `make static_lib` and `sudo make install`)
2. Build: `go build`
3. Run: `./aracno`

## Contribution
Contributions are highly appreciated. Feel free to open a new issue or submit a pull request.
