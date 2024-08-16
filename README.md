# IGinX Zeppelin-Interpreter

## 介绍

这是一个Zeppelin的IGinX解释器，用于连接Zeppelin和IGinX。

## 如何构建

执行下面的命令以构建IGinX Zeppelin-Interpreter的Jar包。

```shell
mvn clean package -DskipTests -P get-jar-with-dependencies
```

1. 把文件 `zeppelin-iginx-0.6.0-SNAPSHOT-jar-with-dependencies.jar` 复制到`$ZEPPELIN_HOME/interpreters/iginx`中（如果文件夹不存在，则创建）。
2. 重启Zeppelin，重启后即可在Zeppelin中查找到IGinX解释器。

# IGinX Zeppelin使用手册

## 编译IGinX Zeppelin Interpreter

进入IGinX目录，执行下面的语句以构建IGinX Zeppelin Interpreter

```Shell
mvn clean package -DskipTests -P get-jar-with-dependencies    
```

构建成功后，在`IGinX/zeppelin-interpreter/target/`路径下找到`zeppelin-iginx-0.6.0-SNAPSHOT-jar-with-dependencies.jar`包。

在下一步部署Zeppelin时我们需要用到这个包。

## 部署Zeppelin

### 方法1：直接下载并部署（推荐）

#### 下载Zeppelin包

前往[Zeppelin官网](https://zeppelin.apache.org/download.html)，下载红框标记出的包：

这两个包的区别是`all`包自带了zeppelin所有的解释器，而`netinst`只带有少量解释器，因为我们使用IGinX自行实现的解释器，下载`netinst`版本即可。（如果0.10.1版本使用有问题的话，找到`zeppelin-0.9.0-preview2`版本下载也可以使用，使用方法相同）

![img](./images/zeppelin_download_page.png)

下载解压后，能够得到`zeppelin-0.10.1-bin-netinst`文件夹，进入其中。

#### 修改Zeppelin设置（可选）

##### 修改Zeppelin端口

Zeppelin默认占用`8080`端口，如果8080端口已被占用，可以按照以下步骤修改Zeppelin端口。

在`zeppelin-0.10.1-bin-netinst/conf/`文件夹下找到文件`zeppelin-site.xml.template`，复制一份并改名为`zeppelin-site.xml`。

编辑文件，在文件中找到下面这段代码，将`8080`修改为自己想要的端口即可。

```Shell
<property>
  <name>zeppelin.server.port</name>
  <value>8080</value>
  <description>Server port.</description>
</property>
```

##### 设置JAVA路径

如果Zeppelin找不到JAVA路径，可以尝试通过以下步骤设置JAVA路径。

在`zeppelin-0.10.1-bin-netinst/conf/`文件夹下找到文件`zeppelin-env.sh.template`，复制一份并改名为`zeppelin-env.sh`。

编辑文件，在文件中找到下面这段代码，在`=`后填入本机的JAVA路径即可。

```Shell
export JAVA_HOME=
```

#### 接入IGinX Zeppelin Interpreter

在`zeppelin-0.10.1-bin-netinst/interpreter/`文件夹下新建一个文件夹`IGinX`，将构建好的`zeppelin-iginx-0.6.0-SNAPSHOT-jar-with-dependencies.jar`包放入其中即可。

#### 启动IGinX

接下来在启动Zeppelin前，我们需要先启动IGinX。

#### 启动Zeppelin

最后，用命令行在`zeppelin-0.10.1-bin-netinst/bin/`中目录下，运行命令：

```Shell
// Unix
./zeppelin-daemon.sh start

// Windows
./zeppelin.cmd
```

即可启动Zeppelin。

#### 常见问题

##### Windows上启动时出现警告日志 

```
java.io.FileNotFoundException: C:\Zeppelin\zeppelin-web-angular\dist
```
参考[这个问答](https://stackoverflow.com/questions/61078714/apache-zeppelin-not-loading-in-a-browser-in-windows-10)解决。

#### Windows上运行，解释器抛出 java.lang.ArrayIndexOutOfBoundsException

参考[这个issue](https://github.com/apache/iotdb/issues/3417)

### 方法2：通过Docker部署

Zeppelin也可以通过docker部署，但通过docker部署后，再修改配置文件、将IGinX-Zeppelin解释器加入都较为麻烦，因此还是推荐第一种做法。

#### 启动IGinX

在部署Zeppelin之前，先启动IGinX。

#### 接入IGinX Zeppelin Interpreter

我们需要准备一个文件夹，用于放置IGinX Zeppelin Interpreter。例如我们准备一个文件夹名为`zeppelin-interpreter`，其绝对路径为`~/code/zeppelin-interpreter/`。

将`zeppelin-iginx-0.6.0-SNAPSHOT-jar-with-dependencies.jar`包放入我们准备好的`zeppelin-interpreter`文件夹内即可。

#### 使用命令启动Docker容器

通过以下代码部署Zeppelin

```Shell
docker run -v ~/code/zeppelin-interpreter/:/opt/zeppelin/interpreter/iginx --privileged=true --name zeppelin --network host apache/zeppelin:0.10.1
```

`-v` 参数是将宿主机的一个文件夹映射到zeppelin容器内，用于放置IGinX Zeppelin Interpreter，在上一步我们已经准备好了这个文件夹。将文件夹绝对路径替换掉红色部分即可。

`--network host` 参数是不使用端口映射，因为zeppelin还需要连接宿主机的IGinX端口，因此直接使用本地端口`8080`。

## 使用Zeppelin

使用浏览器访问http://127.0.0.1:8080/，即可进入Zeppelin。端口号根据自己的设置修改。

### 修改IGinX解释器配置

在启动IGinX解释器前，我们还需要先修改一下配置，在Zeppelin主页面右上角按照下面步骤点击打开解释器设置页面。

<img src="./images/Interpreter-setting-1.png" alt="img" style="zoom:75%;" />

找到IGinX解释器的配置。

![img](./images/Interpreter-Setting-2.png)

对配置进行修改，下面对配置各项进行说明，其中标红的3项需要特别注意，其他一般不需要设置。

1. **iginx.host**：IGinX所在服务器的IP地址，如果Zeppelin与IGinX部署在不同机器，需要进行设置，否则不需要设置。
2. iginx.port：IGinX占用的端口，如果没有改动则不需要设置。
3. iginx.username：IGinX登陆账号，如果没有改动则不需要设置。
4. iginx.password：IGinX登陆密码，如果没有改动则不需要设置。
5. iginx.time.precision：IGinX中时间计算单位，不需要特别设置。
6. **iginx.outfile.dir**：使用OUTFILE语句时，Zeppelin会将文件下载到服务器上的一个中转文件夹，再提供下载连接。此处需要在Zeppelin所在的服务器上创建一个中转文件夹，并填入路径。
7. iginx.fetch.size：IGinX一次能下载的文件数量，默认为1000，如果调大可能会减缓下载速度。
8. iginx.outfile.max.num：IGinX OUTFILE文件夹中存放的总文件夹数量限制，每次查询会创建一个文件夹，超出后会删除最早的文件夹。
9. iginx.outfile.max.size：IGinX OUTFILE文件夹中存储的总文件大小限制，单位为MB，超出后会删除最早的文件。
10. **iginx.file.http.port**：IGinX中文件下载服务要占用的端口，默认为18082，如果需要修改端口则修改此处。

### 新建IGinX笔记本

点击红框内的Create new note

![img](./images/create_new_note.png)

设置note name，并在Default Interpreter中找到iginx

![img](./images/set_new_note.png)

点击create即可创建笔记本。

### 重启解释器

IGinX Zeppelin解释器是需要连接IGinX的，如果我们重启了IGinX，解释器就会断开连接，此时我们需要重启解释器。

在笔记本界面，我们点击红框内的设置按钮。再在出现的页面中，点击iginx解释器左边的刷新按钮，即可重启解释器。

![img](./images/restart_interpreter.png)

### 使用IGinX语句

直接在笔记本中输入IGinX语句即可。

![img](./images/iginx_sql.png)

### 使用RESTful语句

RESTful的curl语句属于shell语句，我们需要用到Zeppelin自带的shell解释器。

使用shell解释器的方法是在第一行增加`%sh`，剩余语句在第二行输入。另外由于Zeppelin不支持传入文件，curl中的json部分需要直接写在语句里。

![img](./images/restful.png)

### 文件下载以及图片展示

支持下载文件，并且可以展示filesystem中的图片

```shell
select * from table into outfile "path" as stream;（默认不展示图片，仅提供文件下载链接）
select * from table into outfile "path" as stream showimg true;（展示fs中的图片，并提供文件下载链接）
select * from table into outfile "path" as stream showimg false;（仅提供文件下载链接）
```