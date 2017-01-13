# Scala Execution Time Monitor
The SETM project is a small wrapper for the [JETM](http://jetm.void.fm/) project
with the goal to provide an easier and more Scala-like API.

## Usage
Simply wrap the timing function around the piece of code that you would like to
monitor:

```Scala
import dbis.setm.SETM._

object Main {
  def greeting(s: String) = timing("greeting func") {
    // complex operations, e.g.
    (0 until 100).foreach(i => println(s"Hello $s"))
   }

  def main(args: Array[String]) {

    start()

    timing("program total") {

      val names = timing("create names") { Array("Tick","Trick","Track") }

      for(name <- names)
        greeting(name)

    }

    collect()
  }
}
```

In the main method, we start the ETM monitor by called `start()` from the class `SETM`.
We then start doing our work and at the end, we `collect` the runtime information.
The call to collect will print the results on the screen.

We define our function of which we want to monitor execution time, called `greeting`.
The method body is wrapped in the call to `timing`. This will create a measurement called
`greeting func`. We can also measure whole blocks of code and value or variable assignments.


You will see an output similar to this:

```
|-------------------|---|---------|-------|-------|-------|
| Measurement Point | # | Average |  Min  |  Max  | Total |
|-------------------|---|---------|-------|-------|-------|
| program total     | 1 |   7.381 | 7.381 | 7.381 | 7.381 |
|   create names    | 1 |   0.028 | 0.028 | 0.028 | 0.028 |
|   greeting func   | 3 |   2.218 | 1.440 | 3.610 | 6.655 |
|-------------------|---|---------|-------|-------|-------|
```

We see that the measurement point `greeting func` was executed three times with an average runtime
of around 2.2 seconds as well as the min/max and total execution times.

## Installation

You can use the sbt `assembly` plugin that is included in the project to build a fat jar file
that you can add to your project.
Alternatively, just add this project as a Git submodule and/or [sbt sub project](http://stackoverflow.com/a/20084950/494428).

## Limitations
Currently, there is no way to configure the underlying JETM monitor.
SETM will use nesting to represent the call stack and print the results to STDOUT
using JETM's [SimpleTextRenderer](http://jetm.void.fm/api/etm/core/renderer/SimpleTextRenderer.html).
This will be addressed soon. 
