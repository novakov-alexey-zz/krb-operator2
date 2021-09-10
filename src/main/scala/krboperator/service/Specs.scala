package krboperator.service

import com.goyeau.kubernetes.client.IntValue
import io.k8s.api.apps.v1.{Deployment, DeploymentSpec}
import io.k8s.api.core.v1._
import io.k8s.apimachinery.pkg.apis.meta.v1.{LabelSelector, ObjectMeta}
import krboperator.KrbOperatorCfg

object Specs {
  def deployment(kdcName: String, krbRealm: String, cfg: KrbOperatorCfg): Deployment =
    Deployment(
      metadata = Some(
        ObjectMeta(
          name = Some(kdcName)
        )
      ),
      spec = Some(
        DeploymentSpec(
          replicas = Some(1),
          selector = LabelSelector(
            matchLabels = Some(Map("deployment" -> kdcName))
          ),
          template = PodTemplateSpec(
            metadata = Some(
              ObjectMeta(
                labels = Some(Map("deployment" -> kdcName))
              )
            ),
            spec = Some(
              PodSpec(
                containers = Seq(
                  Container(
                    image = Some(cfg.krb5Image),
                    imagePullPolicy = Some("Always"),
                    name = "kadmin",
                    env = Some(
                      Seq(
                        EnvVar("RUN_MODE", Some("kadmin")),
                        EnvVar("KRB5_KDC", Some(kdcName)),
                        EnvVar("KRB5_REALM", Some(krbRealm))
                      )
                    ),
                    readinessProbe = Some(
                      Probe(
                        exec =
                          Some(ExecAction(Some(Seq("ls", "/etc/krb5.conf")))),
                        initialDelaySeconds = Some(10),
                        periodSeconds = Some(5)
                      )
                    ),
                    ports = Some(
                      Seq(
                        ContainerPort(
                          containerPort = 8888,
                          protocol = Some("TCP")
                        ),
                        ContainerPort(
                          containerPort = 8888,
                          protocol = Some("UDP")
                        )
                      )
                    ),
                    volumeMounts = Some(
                      Seq(
                        VolumeMount(
                          mountPath = "/dev/shm",
                          name = "share"
                        )
                      )
                    )
                  ),
                  Container(
                    image = Some(cfg.krb5Image),
                    imagePullPolicy = Some("Always"),
                    name = "kdc",
                    env = Some(
                      Seq(
                        EnvVar("RUN_MODE", Some("kdc")),
                        EnvVar("KRB5_KDC", Some(kdcName)),
                        EnvVar("KRB5_REALM", Some(krbRealm))
                      )
                    ),
                    readinessProbe = Some(
                      Probe(
                        exec =
                          Some(ExecAction(Some(Seq("ls", "/etc/krb5.conf")))),
                        initialDelaySeconds = Some(10),
                        periodSeconds = Some(5)
                      )
                    ),
                    ports = Some(
                      Seq(
                        ContainerPort(
                          containerPort = 8749,
                          protocol = Some("TCP")
                        ),
                        ContainerPort(
                          containerPort = 8749,
                          protocol = Some("UDP")
                        ),
                        ContainerPort(
                          containerPort = 8464,
                          protocol = Some("UDP")
                        )
                      )
                    ),
                    volumeMounts = Some(
                      Seq(
                        VolumeMount(
                          mountPath = "/dev/shm",
                          name = "share"
                        ),
                        VolumeMount(
                          mountPath = "/var/kerberos/krb5kdc.d",
                          name = "kdc-config"
                        ),
                        VolumeMount(
                          mountPath = "/etc/krb.conf.d",
                          name = "krb5-config"
                        ),
                        VolumeMount(
                          mountPath = "/etc/krb5/secret/krb5_pass",
                          subPath = Some("krb5_pass"),
                          name = "admin-secret"
                        )
                      )
                    )
                  )
                ),
                dnsPolicy = Some("ClusterFirst"),
                restartPolicy = Some("Always"),
                terminationGracePeriodSeconds = Some(30),
                volumes = Some(
                  Seq(
                    Volume(
                      name = "share",
                      emptyDir =
                        Some(EmptyDirVolumeSource(medium = Some("share")))
                    ),
                    Volume(
                      name = "kdc-config",
                      emptyDir = Some(EmptyDirVolumeSource())
                    ),
                    Volume(
                      name = "krb5-config",
                      emptyDir = Some(EmptyDirVolumeSource())
                    ),
                    Volume(
                      name = "admin-secret",
                      secret = Some(
                        SecretVolumeSource(secretName =
                          Some(cfg.k8sResourcesPrefix + "-krb-admin-pwd")
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )

  def service(kdcName: String): Service =
    Service(
      metadata = Some(ObjectMeta(name = Some(kdcName))),
      spec = Some(
        ServiceSpec(
          ports = Some(
            Seq(
              ServicePort(
                name = Some("kerberos-kdc-tcp"),
                port = 88,
                protocol = Some("TCP"),
                targetPort = Some(IntValue(8888))
              ),
              ServicePort(
                name = Some("kerberos-kdc"),
                port = 88,
                protocol = Some("UDP"),
                targetPort = Some(IntValue(8888))
              ),
              ServicePort(
                name = Some("kpasswd"),
                port = 464,
                protocol = Some("UDP"),
                targetPort = Some(IntValue(8464))
              ),
              ServicePort(
                name = Some("kadmin"),
                port = 749,
                protocol = Some("UDP"),
                targetPort = Some(IntValue(8749))
              ),
              ServicePort(
                name = Some("kadmin-tcp"),
                port = 749,
                protocol = Some("TCP"),
                targetPort = Some(IntValue(8749))
              )
            )
          ),
          selector = Some(
            Map("deployment" -> kdcName)
          ),
          sessionAffinity = None,
          `type` = Some("ClusterIP")
        )
      )
    )
}
