# Fint Core Consumer

## Description
The Fint Core Consumer project is designed to streamline and unify our existing consumer projects into a single, cohesive solution. By leveraging this project, you can easily manage and modify consumer behavior through configuration, reducing the complexity of maintaining multiple, separate consumers.

## How it works
This project uses the configuration settings provided to dynamically determine the domain and package of the resources to be handled. Java Reflection is then employed to scan and manage these resources, enabling the application to automatically serialize and deserialize Fint resource objects as needed in the REST API.

## Customization
To customize the project to use resources from another FINT component all we simply have to do is change the configuration or set two environment variables.
```yaml
fint:
  consumer:
    domain: utdanning
    package: vurdering
```
