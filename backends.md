## Backends ##

In our context, a backend is some library that can be used to translate the Pig script into code for the corresponding platform/framework. The backend is also responsible for starting the created job.

### General information ###

We already support various backends that can be used out of the box. Currently, we provide a [Apache Spark](http://spark.apache.org/) and a [Apache Flink](http://flink.apache.org/) backend.

These backends are **not** hard-coded into Piglet. All you have to do to add a backend to Piglet is to add the backends jar file to the classpath before starting Piglet. If you use the wrapper scripts to start Piglet, you can set the `BACKEND_DIR` variable to point to the file in include. The content of this variable will be added to the `CLASSPATH`.


### Creating backends ###

Piglet comes with a subproject called `common`. This project currently contains two traits that need to be implemented by a backend library and they make sure, that the main project can interact with your new backend.

To start, run `sbt package` in the `common` direcory to build this subproject. You can also run the command in the Piglet directory to build all projects. Now you have to include the generated `common/target/scala-2.11/common_2.11-1.0.jar` as a dependency to your backend project.

In your code, you must implement the two provided traits.

  * **BackendConf**
    Piglet will use this class to ask about the configuration of your backend. This trait defines two functions:
    * `runnerClass`: This method should return an instance of a class implementing the `PigletBackend` trait. We will use this class to start the job.
    * `templateFile`: This is the name of the [scalasti](https://github.com/bmc/scalasti) template file that will be used for code generation.

  * **PigletBackend**
    This trait provides only one method `execute` and we will call this method on an instance of this class to use your backend to execute the current job. Here, you will put your code to start a new job/task for your platform.

Now, that you have implemented your backend code, you need to add some configuration to Piglet, to let us know, that your backend plug-in exists.
To do so, edit the configuration file and add a new entry under `backends`. The new key must be the name of your backend, i.e., how you want to call it with the `-b` option.

Let's say your backend is named 'awesome', then you create a new entry like this:

```
backends {
  awesome {
    jar = "path/to/backend.jar"
    conf = "my.awesome.package.ClassImplementingBackendConf"
  }

  // other backends like Spark, Flink, ...
}
```

The `jar` key gives the path to the jar file of your backend and `conf` gives the full qualified name of your class that implements the `BackendConf` trait.

You can now start Piglet with `-b awesome`

Piglet will then load an instance of the class specified in `backends.<backend>.conf` and get all further information from there. Where `<backend>` is the value that was passed via `-b` option.
