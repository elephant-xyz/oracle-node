declare module "@elephant-xyz/cli/lib" {
  export interface TransformParams {
    inputZip: string;
    outputZip: string;
    scriptsZip?: string;
    cwd: string;
  }
  export interface ValidateParams {
    input: string;
    cwd: string;
  }
  export interface HashParams {
    input: string;
    outputZip: string;
    outputCsv: string;
    propertyCid?: string;
    cwd: string;
  }
  export interface UploadParams {
    input: string;
    pinataJwt: string;
    cwd: string;
  }
  export interface CliResult {
    success: boolean;
    error?: string;
  }

  export function transform(params: TransformParams): Promise<CliResult>;
  export function validate(params: ValidateParams): Promise<CliResult>;
  export function hash(params: HashParams): Promise<CliResult>;
  export function upload(params: UploadParams): Promise<CliResult>;
}




