import { ApplicationConfig, importProvidersFrom } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient, withInterceptorsFromDi, HTTP_INTERCEPTORS } from '@angular/common/http';
import {
  MsalModule,
  MsalService,
  MsalGuard,
  MsalBroadcastService,
  MsalInterceptor,
  MSAL_INSTANCE,
  MSAL_GUARD_CONFIG,
  MSAL_INTERCEPTOR_CONFIG,
  MsalGuardConfiguration,
  MsalInterceptorConfiguration
} from '@azure/msal-angular';
import {
  PublicClientApplication,
  InteractionType,
  IPublicClientApplication
} from '@azure/msal-browser';
import { environment } from '../environments/environment';
import { routes } from './app.routes';

// Crea la instancia de MSAL con la config de environment
export function MSALInstanceFactory(): IPublicClientApplication {
  return new PublicClientApplication(environment.msalConfig);
}

// Config del guard: redirige al login si la ruta protegida lo requiere
export function MSALGuardConfigFactory(): MsalGuardConfiguration {
  return {
    interactionType: InteractionType.Redirect,
    authRequest: {
      scopes: environment.apiConfig.scopes
    }
  };
}

// Config del interceptor: adjunta el token JWT a las llamadas hacia cada microservicio
export function MSALInterceptorConfigFactory(): MsalInterceptorConfiguration {
  const protectedResourceMap = new Map<string, Array<string>>();

  // Se registra cada microservicio para que el interceptor adjunte el token en todos
  protectedResourceMap.set(`${environment.apiConfig.pedidosUri}/*`, environment.apiConfig.scopes);
  protectedResourceMap.set(`${environment.apiConfig.productosUri}/*`, environment.apiConfig.scopes);
  protectedResourceMap.set(`${environment.apiConfig.clientesUri}/*`, environment.apiConfig.scopes);
  protectedResourceMap.set(`${environment.apiConfig.notificacionesUri}/*`, environment.apiConfig.scopes);
  protectedResourceMap.set(`${environment.apiConfig.analiticaUri}/*`, environment.apiConfig.scopes);

  return {
    interactionType: InteractionType.Redirect,
    protectedResourceMap
  };
}

export const appConfig: ApplicationConfig = {
  providers: [
    provideRouter(routes),
    provideHttpClient(withInterceptorsFromDi()),
    importProvidersFrom(MsalModule),
    {
      provide: HTTP_INTERCEPTORS,
      useClass: MsalInterceptor,
      multi: true
    },
    {
      provide: MSAL_INSTANCE,
      useFactory: MSALInstanceFactory
    },
    {
      provide: MSAL_GUARD_CONFIG,
      useFactory: MSALGuardConfigFactory
    },
    {
      provide: MSAL_INTERCEPTOR_CONFIG,
      useFactory: MSALInterceptorConfigFactory
    },
    MsalService,
    MsalGuard,
    MsalBroadcastService
  ]
};