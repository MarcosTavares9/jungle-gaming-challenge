import {
  Injectable,
  type CanActivate,
  type ExecutionContext,
} from "@nestjs/common";

// Extension point for an OIDC provider identity adapter. See ARCHITECTURE.md.
@Injectable()
export class ProviderIdentityGuard implements CanActivate {
  canActivate(_context: ExecutionContext): boolean {
    return true;
  }
}
