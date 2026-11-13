export const environment = {
  production: false,
  msalConfig: {
    auth: {
      clientId: 'ac0cd280-33b6-4d33-ae42-aa056c7029fa',
      authority: 'https://login.microsoftonline.com/14c021dd-6b1d-423e-a9f9-e59b17394821',
      redirectUri: 'http://localhost:4200/'
    }
  },
  apiConfig: {
    scopes: ['api://ac0cd280-33b6-4d33-ae42-aa056c7029fa/access_as_user'],
    // TEMPORAL (prueba local): pedidos-service en mi PC. Valor original: la misma URL de pedidosUri.
    uri: 'http://localhost:8081',
    pedidosUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/pedidos',
    productosUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/productos',
    clientesUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/clientes',
    notificacionesUri: 'https://wrxinrzh5h.execute-api.us-east-1.amazonaws.com/notificaciones',
    // Local: analitica-service directo. En AWS se reemplaza por la ruta del API Gateway (/analitica).
    analiticaUri: 'http://localhost:8087'
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