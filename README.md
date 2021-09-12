# Kerberos Operator 2

**UNDER DEVELOPMENT**

New version of Kerberos Operator, but this time completely written in Scala with Cats-Effects.

This operator deployes KDC, Kadmin servers and creates principals and their keytabs as Kubernetes secrets.
Developed using [Kubernetes-Client](https://github.com/joan38/kubernetes-client) Scala library.

## Operator use cases

Why to use this Operator?

-   Your [SPNEGO](https://en.wikipedia.org/wiki/SPNEGO) authentication requires keytab mounted to a Pod: 
deploy this operator with required principals to get automatically created secrets with keytabs inside
    
-   Rapid application development having KDC running inside the K8s cluster: deploy this operator and use 
automatically created service to call KDC or Kadmin servers

-   Principals and keytabs management using K8s custom resources: deploy this operator using Krb resource
with required list of principals, and their predefined or random passwords 

## How to install

### Prerequisites

-   kubectl CLI
-   [dhall CLI](https://docs.dhall-lang.org/tutorials/Getting-started_Generate-JSON-or-YAML.html#installation)

### Installation Steps

Define namespace as environment variable:

```bash
export NAMESPACE=<put desired namespace>
```

```bash
# install RBAC
wget -O- -q https://raw.githubusercontent.com/novakov-alexey/krb-operator2/master/manifest/rbac.dhall | \
    dhall-to-yaml | kubectl create -n ${NAMESPACE} -f -

# install operator
wget -O- -q https://raw.githubusercontent.com/novakov-alexey/krb-operator2/master/manifest/kube-deployment.dhall | \
    dhall-to-yaml | kubectl create -n ${NAMESPACE} -f -
```

Alternatively, just clone this repository and run `make install` in root folder of the repository.
Run `make uninstall` to uninstall Kerberos Operator.


### Deploy Specific Operator Version

In order to deploy a specific version, clone above manifest files and change the image tag in the `krb-operator2` container. 
For example:

```diff
-image: alexeyn/kerberos-operator:0.1.1
+image: alexeyn/kerberos-operator:0.1.1
```

## How to uninstall

```bash
wget -O- -q https://raw.githubusercontent.com/novakov-alexey/krb-operator2/master/manifest/rbac.dhall | \
 		NAMESPACE=${NAMESPACE} dhall-to-yaml | kubectl delete -n ${NAMESPACE} -f -
	
wget -O- -q https://raw.githubusercontent.com/novakov-alexey/krb-operator2/master/manifest/kube-deployment.dhall | \
		NAMESPACE=${NAMESPACE} dhall-to-yaml | kubectl delete -n ${NAMESPACE} -f -

kubectl delete crd krbservers.krb-operator.novakov-alexey.github.io
kubectl delete crd principalss.krb-operator.novakov-alexey.github.io
```

## Custom Resource Definitions

### KrbServer

Below resource creates:
-   `KDC` and `Kadmin` servers running as two separate containers running in a single Pod 

```yaml
apiVersion: krb-operator.novakov-alexey.github.io/v1
kind: KrbServer
metadata:
  name: my-krb
  namespace: test
spec:
  realm: EXAMPLE.COM  
```

#### KrbServer Spec

`realm` - Kerberos realm where all principals will be created

### PrincipalList

Below resource creates:
-   Principals and their keytabs based on the principal list

```yaml
apiVersion: krb-operator.novakov-alexey.github.io/v1
kind: Principals
metadata:
  name: my-krb1
  namespace: test
  labels:
    krb-operator.novakov-alexey.github.io/server: my-krb # reference to KrbServer
spec:
  list:
    - name: client1
      password:
        type: static
        value: mypass
      keytab: cluster.keytab
      secret:
        type: Keytab
        name: cluster-keytab
    - name: user2
      keytab: cluster.keytab
      secret:
        type: KeytabAndPassword
        name: cluster-keytab
```

#### PrincipalList Spec
 
-   `list` - array of principals 

Principal has the following properties:

-   `name` - principal name without realm in it. Realm will be added automatically using value of `spec.realm` property

-   `password` - a property with two different types. 

    `static`: with password in the value field. 
    
    `random`: operator generates random password. it does not require password property in the resource at all.    
    
    Missing password property or default value is `random`.     

-   `keytab` - it is key in the secret object. Secret can have more than one data keys, i.e. more than one keytab files

-   `secret` - a property with two different types. 
    
    `Keytab` - create keytab as K8s Secret, `name` is the Secret name.
    
    `KeytabAndPassword` - create keytab with separate password entry as K8s Secret, `name` is the Secret name, 
    `principal[i].name` is a key of a secret for principal password
     
     K8s secret name. Every principal in the array can have its own secret name, so that multiple secrets will be created

## Kubernetes objects

If you apply above two custom resources as is, then it will produce the following objects 
in the metadata.namespace, i.e. `test` namespace:

### Secret

Containing Kerberos keytab as secret data:

```bash
kubectl describe secret cluster-keytab  -n test
Name:         cluster-keytab
Namespace:    test
Labels:       app=krb
Annotations:  <none>

Type:  opaque

Data
====
cluster.keytab:  274 bytes
user2:           10 bytes
```

Property `principals.secret` in the `Krb` spec can be different, so that it will lead to multiple/different 
secrets created by the Kerberos Operator.

### Service

A Service for KDC, kadmin, kpasswd with their TCP/UDP ports:  

```bash
my-krb   ClusterIP   172.30.37.134  <none>  88/TCP,88/UDP,464/UDP,749/UDP,749/TCP
```

### Pod

Krb Pod for `KDC`, `kadmin`, `kpasswd` servers is deployed with two containers:

```bash
kubectl get pod my-krb-1-gk52x -n test

NAME             READY   STATUS    RESTARTS   AGE
my-krb-1-gk52x   2/2     Running   0          24m
```

Krb Pod is deployed as Deployment resource:

```bash
NAME                     READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/my-krb   1/1     1            1           26s
```

## Create or Update resource

Examples:

```bash
kubectl create -f examples/my-krb-1.yaml
# or
kubectl apply -f examples/my-krb-1.yaml
```

A `create` or `update` resource event are handled in the same way. They will create:

-   Deployment, Service, Pod, if some of them is missing

-   Kerberos principal, if its `spec.list[i].secret` is missing. 
    Changes in values other than `secret` are ignored (current limitation). In order to add new principal to the 
    `spec.principals` either put new/not-existing `secret` name and desired new principal name. Otherwise, delete Krb resource and create new one with 
    the desired `principals`.   

## Delete resource

A `delete` event deletes all objects created by the `create` or `apply` events: Deployment, Service, Pod and Secrets(s)

```bash
kubectl delete -f examples/my-krb-1.yaml
```

## Build locally

```bash
sbt docker:publishLocal
```

Then use your built image in `manifest/*-deployment.dhall` file for `krb-operator` container.
