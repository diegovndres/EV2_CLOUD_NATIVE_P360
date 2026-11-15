import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface NuevoPedido {
  cliente: string;
  producto: string;
  cantidad: number;
  estado: string;
  fecha: string;
}

@Injectable({ providedIn: 'root' })
export class PedidosService {
  constructor(private http: HttpClient) {}

  listar(): Observable<any[]> {
    return this.http.get<any[]>(`${environment.apiConfig.uri}/api/pedidos`);
  }

  crear(pedido: NuevoPedido): Observable<any> {
    return this.http.post<any>(`${environment.apiConfig.uri}/api/pedidos`, pedido);
  }
}