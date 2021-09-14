package krboperator

import cats.syntax.option._
import com.goyeau.kubernetes.client.crd.{CrdContext, JSON, SchemaNotArrayValue}
import io.k8s.apiextensionsapiserver.pkg.apis.apiextensions.v1._
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta

import scala.reflect.ClassTag

object crds {

  val group = "krb-operator.novakov-alexey.github.io"
  val crdLabel = Map("managedBy" -> "kerberos-operator2")

  def context[A: ClassTag] = {
    val kind = implicitly[ClassTag[A]].runtimeClass.getSimpleName
    val plural = s"${kind.toLowerCase}s"
    CrdContext(
      group,
      "v1",
      plural
    ) //TODO: version to extract to configuration
  }

  def plural(resourceName: String): String = s"${resourceName.toLowerCase}s"
  def crdName(ctx: CrdContext): String =
    s"${ctx.plural}.${ctx.group}"

  object server {
    val kind = "KrbServer"

    def version(number: String): CustomResourceDefinitionVersion =
      CustomResourceDefinitionVersion(
        name = number,
        served = true,
        storage = true,
        schema = CustomResourceValidation(
          JSONSchemaProps(
            `type` = "object".some,
            properties = Map(
              "spec" -> JSONSchemaProps(
                `type` = "object".some,
                properties = Map(
                  "realm" -> JSONSchemaProps(`type` = "string".some)
                ).some
              ),
              "status" -> JSONSchemaProps(
                `type` = "object".some,
                properties = Map(
                  "processed" -> JSONSchemaProps(`type` = "boolean".some),
                  "error" -> JSONSchemaProps(`type` = "string".some)
                ).some,
                required = Seq("processed").some
              )
            ).some
          ).some
        ).some,
        subresources = CustomResourceSubresources(status =
          CustomResourceSubresourceStatus().some
        ).some
      )

    def definition(ctx: CrdContext) = CustomResourceDefinition(
      spec = CustomResourceDefinitionSpec(
        group = ctx.group,
        scope = "Namespaced",
        names = CustomResourceDefinitionNames(
          plural(kind),
          kind
        ),
        versions = Seq(version(ctx.version))
      ),
      apiVersion = "apiextensions.k8s.io/v1".some,
      metadata = ObjectMeta(
        name = crdName(ctx).some,
        labels = crdLabel.some
      ).some
    )

  }

  object principal {
    val kind = "Principals"

    def version(number: String): CustomResourceDefinitionVersion =
      CustomResourceDefinitionVersion(
        name = number,
        served = true,
        storage = true,
        schema = CustomResourceValidation(
          JSONSchemaProps(
            `type` = "object".some,
            properties = Map(
              "spec" -> JSONSchemaProps(
                `type` = "object".some,
                properties = Map(
                  "list" -> JSONSchemaProps(
                    `type` = "array".some,
                    items = SchemaNotArrayValue(
                      JSONSchemaProps(
                        `type` = "object".some,
                        properties = Map(
                          "password" -> JSONSchemaProps(
                            `type` = "object".some,
                            properties = Map(
                              "type" -> JSONSchemaProps(`type` = "string".some),
                              "value" -> JSONSchemaProps(`type` = "string".some)
                            ).some,
                            oneOf = Seq(
                              JSONSchemaProps(
                                properties = Map(
                                  "type" -> JSONSchemaProps(`enum` =
                                    Seq(JSON("static"), JSON("random")).some
                                  )
                                ).some
                              )
                            ).some
                          ),
                          "name" -> JSONSchemaProps(
                            `type` = "string".some
                          ),
                          "keytab" -> JSONSchemaProps(
                            `type` = "string".some
                          ),
                          "secret" -> JSONSchemaProps(
                            `type` = "object".some,
                            properties = Map(
                              "type" -> JSONSchemaProps(`type` = "string".some),
                              "name" -> JSONSchemaProps(`type` = "string".some)
                            ).some,
                            oneOf = Seq(
                              JSONSchemaProps(
                                properties = Map(
                                  "type" -> JSONSchemaProps(`enum` =
                                    Seq(
                                      JSON("Keytab"),
                                      JSON("KeytabAndPassword")
                                    ).some
                                  )
                                ).some,
                                required = Seq("type").some
                              )
                            ).some
                          )
                        ).some
                      )
                    ).some
                  )
                ).some
              ),
              "status" -> JSONSchemaProps(
                `type` = "object".some,
                properties = Map(
                  "processed" -> JSONSchemaProps(`type` = "boolean".some),
                  "lastPrincipalCount" -> JSONSchemaProps(`type` =
                    "integer".some
                  ),
                  "totalPrincipalCount" -> JSONSchemaProps(`type` =
                    "integer".some
                  ),
                  "error" -> JSONSchemaProps(`type` = "string".some)
                ).some,
                required = Seq(
                  "processed",
                  "lastPrincipalCount",
                  "totalPrincipalCount"
                ).some
              )
            ).some
          ).some
        ).some,
        subresources = CustomResourceSubresources(status =
          CustomResourceSubresourceStatus().some
        ).some
      )

    def definition(ctx: CrdContext) = CustomResourceDefinition(
      spec = CustomResourceDefinitionSpec(
        group = ctx.group,
        scope = "Namespaced",
        names = CustomResourceDefinitionNames(
          plural(kind),
          kind
        ),
        versions = Seq(version(ctx.version))
      ),
      apiVersion = "apiextensions.k8s.io/v1".some,
      metadata = ObjectMeta(
        name = crdName(ctx).some,
        labels = crdLabel.some
      ).some
    )
  }

}
