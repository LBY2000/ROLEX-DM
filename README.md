# ROLEX-DM
This repository aims for ROLEX-DM, which is running on the true disaggregated memory system



<b>1.由于Node11上的boost库环境问题，在Node11上运行的部分CMakeLisits.txt请替换为Node11_CMakeLists.txt的内容，修改boost库路径</b>

<b>2.node10有其他人在跑程序，需要gcc4.9版本，我们的需要gcc 7以上，因此node10的CMakeLists.txt现在也需要进行替换，修改gcc编译器路径。请替换为node10_CMakeLists.txt的内容    </b>

<b>3.如果出现rdma设备不能用的情况，记得以sudo的方式执行程序    </b>

<b>4. 如果在node22上运行请将此处代码的port替换为1，可以利用ibstatus查看,node22是port1为active</b>
https://github.com/LBY2000/ROLEX-DM/blob/571ae997515c7dc1e753b23ba6efc5d4abd92ffe/Sherman/include/Rdma.h#L60
<b>待补充....</b>
