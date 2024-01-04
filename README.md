# ROLEX-DM
This repository aims for ROLEX-DM, which is running on the true disaggregated memory system



<b>1.由于Node11上的boost库环境问题，在Node11上运行的部分CMakeLisits.txt请替换为Node11_CMakeLists.txt的内容，修改boost库路径</b>

<b>2.node10有其他人在跑程序，需要gcc4.9版本，我们的需要gcc 7以上，因此node10的CMakeLists.txt现在也需要进行替换，修改gcc编译器路径。请替换为node10_CMakeLists.txt的内容    </b>

<b>3.如果出现rdma设备不能用的情况，记得以sudo的方式执行程序    </b>

<b>4. 如果在node22上运行请将如下替换</b>

[XIndex-DM/Sherman/include/Rdma.h at master · LBY2000/XIndex-DM (github.com)#L60](https://github.com/LBY2000/XIndex-DM/blob/master/Sherman/include/Rdma.h)





