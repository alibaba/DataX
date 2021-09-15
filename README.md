# StarRocks writer for DataX

This is a repo forked from [DataX](https://github.com/alibaba/DataX), and maintained by [StarRocks](https://www.starrocks.com) (starrockswriter).

## How to use

1. Run `./build.sh` to gennerate the `starrockswriter.tar.gz`, then untar it into your own [DataX release](https://github.com/alibaba/DataX) directory(which will be `datax/plugin/writer/`).
2. Create a `job.json` to define the reader and writer. More details about the configurations, please refer to the [Documentations](https://docs.starrocks.com).
3. Run `python datax/bin/datax.py --jvm="-Xms6G -Xmx6G" --loglevel=debug job.json` to start a job.
