## Architecture of metalmq client

The client architecture follows the hexagonal architecture in order that we can
easily describe the business logic or better to say protocol logic.

```
+-----------------------------------------------------------+
| Infrastructure                                            |
|                                                           |
| --> client_api                +----------+                |
|                 --> processor |  state   | --> processor  |
|                               |  error   |                |
| --> channel_api               |  model   |                |
|                               +----------+                |
|                                                           |
+-----------------------------------------------------------+
```
