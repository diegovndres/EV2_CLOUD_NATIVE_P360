import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class NotificacionesService {
  constructor(private http: HttpClient) {}

  listar(): Observable<any[]> {
    return this.http.get<any[]>(`${environment.apiConfig.notificacionesUri}/api/notificaciones`);
  }

  crear(notificacion: any): Observable<any> {
    return this.http.post<any>(`${environment.apiConfig.notificacionesUri}/api/notificaciones`, notificacion);
  }

  /** Productor de mensajes: el backend lo publica en RabbitMQ (responde 202 Accepted). */
  enviar(destinatario: string, mensaje: string): Observable<{ estado: string; mensaje: string }> {
    return this.http.post<{ estado: string; mensaje: string }>(
      `${environment.apiConfig.notificacionesUri}/api/notificaciones/enviar`,
      { destinatario, mensaje }
    );
  }
}
