export NAMESPACE=test

create-krb-server:
	kubectl create -f examples/my-krb-server-1.yaml -n $(NAMESPACE)

create-krb-principals:
	kubectl create -f examples/my-principals-1.yaml -n $(NAMESPACE)