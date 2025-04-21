import { useContext } from 'react';
import { AuthContext } from '../Authentication/AuthContext';


export function useAuth() {
  return useContext(AuthContext);
}