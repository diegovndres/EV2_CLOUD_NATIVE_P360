export const environment = {
  production: false,
  msalConfig: {
    auth: {
      clientId: 'ac0cd280-33b6-4d33-ae42-aa056c7029fa',
      authority: 'https://login.microsoftonline.com/14c021dd-6b1d-423e-a9f9-e59b17394821',
      redirectUri: window.location.origin + '/'
    }
  },
  apiConfig: {
    scopes: ['api://ac0cd280-33b6-4d33-ae42-aa056c7029fa/access_as_user'],
    uri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/pedidos',
    pedidosUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/pedidos',
    productosUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/productos',
    clientesUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/clientes',
    notificacionesUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/notificaciones',
    // Analitica a traves del API Gateway (ruta /analitica -> EC2:8087)
    analiticaUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/analitica'
  },
  system: {
    loggerOptions: {
      loggerCallback: (level: any, message: string) => {
        console.log(message);
      },
      logLevel: 3, // Verbose
      piiLoggingEnabled: false
    }
  }
}