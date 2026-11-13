import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface MetricaProducto {
  producto: string;
  totalPedidos: number;
  unidadesVendidas: number;
  promedioUnidadesPorPedido: number;
  actualizadoEn: string;
}

@Injectable({ providedIn: 'root' })
export class AnaliticaService {
  constructor(private http: HttpClient) {}

  /** Metricas por producto calculadas por analitica-service desde el topico Kafka pedidos-eventos. */
  productos(): Observable<MetricaProducto[]> {
    return this.http.get<MetricaProducto[]>(`${environment.apiConfig.analiticaUri}/api/analitica/productos`);
  }
}