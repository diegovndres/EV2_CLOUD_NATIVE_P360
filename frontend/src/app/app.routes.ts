import { Routes } from '@angular/router';
import { MsalGuard } from '@azure/msal-angular';
import { Home } from './pages/home/home';
import { Pedidos } from './pages/pedidos/pedidos';
import { Productos } from './pages/productos/productos';
import { Clientes } from './pages/clientes/clientes';
import { Notificaciones } from './pages/notificaciones/notificaciones';
import { Analitica } from './pages/analitica/analitica';

export const routes: Routes = [
  { path: '', redirectTo: 'home', pathMatch: 'full' },
  { path: 'home', component: Home, canActivate: [MsalGuard] },
  { path: 'pedidos', component: Pedidos, canActivate: [MsalGuard] },
  { path: 'productos', component: Productos, canActivate: [MsalGuard] },
  { path: 'clientes', component: Clientes, canActivate: [MsalGuard] },
  { path: 'notificaciones', component: Notificaciones, canActivate: [MsalGuard] },
  { path: 'analitica', component: Analitica, canActivate: [MsalGuard] }
];