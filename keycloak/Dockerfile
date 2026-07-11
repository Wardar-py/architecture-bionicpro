# Keycloak с расширением российских Identity Provider'ов (Яндекс ID и др.)
# https://github.com/playa-ru/keycloak-russian-providers
FROM quay.io/keycloak/keycloak:24.0.3 AS builder

ARG RUSSIAN_PROVIDERS_VERSION=24.0.1.rsp-3
ADD --chown=keycloak:keycloak \
    https://repo1.maven.org/maven2/ru/playa/keycloak/keycloak-russian-providers/${RUSSIAN_PROVIDERS_VERSION}/keycloak-russian-providers-${RUSSIAN_PROVIDERS_VERSION}.jar \
    /opt/keycloak/providers/keycloak-russian-providers.jar

RUN /opt/keycloak/bin/kc.sh build

FROM quay.io/keycloak/keycloak:24.0.3
COPY --from=builder /opt/keycloak/ /opt/keycloak/

ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]
