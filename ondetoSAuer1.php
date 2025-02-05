<?php
date_default_timezone_set('America/Bahia');
// Função para ler o arquivo e converter em array associativo
function carregarDados($arquivo)
{
    $dados = [];
    if (file_exists($arquivo)) {
        $linhas = file($arquivo, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        foreach ($linhas as $linha) {
            $partes = explode(';', $linha);
            if (count($partes) >= 6) { // Ajustado para incluir dataRetorno como quarto campo
                $dados[] = [
                    'nome' => trim($partes[0]),
                    'foto' => trim($partes[1]),
                    'local' => trim($partes[2]),
                    'dataRetorno' => trim($partes[3]),
                    'timestamp' => isset($partes[4]) ? trim($partes[4]) : date('Y-m-d H:i:s'),
                    'funcao' => isset($partes[5]) ? trim($partes[5]) : '',
                ];
            }
        }
    } else {
        echo "<p class='alert alert-danger'>Arquivo <strong>$arquivo</strong> não encontrado.</p>";
    }
    return $dados;
}

// Função para atualizar o arquivo
function atualizarLocal($arquivo, $nome, $novoLocal, $novaObs, $dataRetorno)
{
    if (file_exists($arquivo) && is_writable($arquivo)) {
        $linhas = file($arquivo, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        $atualizado = false;
        foreach ($linhas as &$linha) {
            $partes = explode(';', $linha);
            if (trim($partes[0]) === $nome) {
                $partes[2] = $novoLocal;
                if ($novoLocal === 'Férias/Folga') {
                    $partes[3] = trim($dataRetorno); // Data de Retorno no quarto campo
                } else {
                    $partes[3] = trim($novaObs); // Observação no quarto campo
                }
                $partes[4] = date('Y-m-d H:i:s'); // Atualiza o timestamp
                $linha = implode(';', $partes);
                $atualizado = true;
                break;
            }
        }
        if ($atualizado) {
            file_put_contents($arquivo, implode(PHP_EOL, $linhas));
            return true;
        }
    }
    return false;
}

// Lida com a submissão do formulário de atualização
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $nome = $_POST['nome'] ?? '';
    $novoLocal = $_POST['novoLocal'] ?? '';
    $novaObs = $_POST['novaObs'] ?? '';
    $dataRetorno = $_POST['dataRetorno'] ?? '';
    if ($nome && $novoLocal) {
        if ($novoLocal === 'Férias/Folga' && empty($dataRetorno)) {
            header("Location: ondeto.php?error=2"); // Erro: Data de Retorno não fornecida
        } else {
            if (atualizarLocal('ondeestou.cfg', $nome, $novoLocal, $novaObs, $dataRetorno)) {
                header("Location: ondeto.php");
            } else {
                header("Location: ondeto.php?error=1");
            }
        }
        exit;
    }
}

// Lida com requisições AJAX
if (isset($_GET['action']) && $_GET['action'] === 'update') {
    $nome = $_GET['nome'] ?? '';
    $novoLocal = $_GET['novoLocal'] ?? '';
    $novaObs = $_GET['novaObs'] ?? '';
    $dataRetorno = $_GET['dataRetorno'] ?? '';
    if ($nome && $novoLocal) {
        if ($novoLocal === 'Férias/Folga' && empty($dataRetorno)) {
            echo json_encode(['success' => false, 'message' => 'Data de Retorno não fornecida.']);
            exit;
        }
        if (atualizarLocal('ondeestou.cfg', $nome, $novoLocal, $novaObs, $dataRetorno)) {
            $pessoas = carregarDados('ondeestou.cfg');
            $pessoaAtualizada = array_filter($pessoas, function ($p) use ($nome) {
                return $p['nome'] === $nome;
            });
            $pessoaAtualizada = reset($pessoaAtualizada);
            echo json_encode([
                'success' => true,
                'local' => $novoLocal,
                'obs' => $novoLocal === 'Férias/Folga' ? $pessoaAtualizada['dataRetorno'] : $novaObs,
                'timestamp' => $pessoaAtualizada['timestamp']
            ]);
        } else {
            echo json_encode(['success' => false, 'message' => 'Falha ao atualizar o arquivo.']);
        }
        exit;
    }
    echo json_encode(['success' => false, 'message' => 'Dados inválidos.']);
    exit;
}

// Função para obter ícone com base no local
function getIconForLocation($local)
{
    $local = strtolower($local);
    if (in_array($local, ['atendimento', 'almoço', 'reunião / palestra', 'fmv'])) {
        return '<i class="bi bi-exclamation-triangle-fill text-warning"></i>';
    } elseif ($local === 'gac') {
        return '<i class="bi bi-check-circle-fill text-success"></i>';
    } elseif (in_array($local, ['férias/folga', 'licença'])) {
        return '<i class="bi bi-x-circle-fill text-danger"></i>';
    }
    return '';
}

// Carrega os dados do arquivo
$arquivo = 'ondeestou.cfg';
$pessoas = carregarDados($arquivo);

// Pega o nome da primeira linha
$primeira_linha = file($arquivo)[0];
$primeiro_nome = explode(';', $primeira_linha)[0];


?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="cache-control" content="no-cache, no-store, must-revalidate">
    <meta http-equiv="pragma" content="no-cache">
    <meta http-equiv="expires" content="0">
    <title>Onde Estão!</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css" rel="stylesheet">
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@popperjs/core@2.9.3/dist/umd/popper.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.min.js"></script>
    <style>
        .person-item {
            display: flex;
            align-items: center;
            margin-bottom: 15px;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 5px;
        }

        .person-item.gerente {
            background-color: #E8F4F8 !important;
        }

        .person-item.plansul {
            background-color: #f5e1fa !important;
        }

        .person-item.fila-1 {
            background-color: #F4FCF4 !important;
        }

        .person-item.fila-2 {
            background-color: #FFF6F6 !important;
        }

        .person-item:first-child {
            background-color: #F5FBFF;
        }

        .first-row {
            width: 100%;
            display: flex;
            padding-left: 15px;
        }

        .first-row>.col-md-6 {
            flex: 0 0 50%;
            max-width: 50%;
        }

        .person-item img {
            width: 50px;
            height: 50px;
            border-radius: 50%;
            margin-right: 15px;
        }

        .person-details {
            flex: 1;
        }

        .person-details h5 {
            margin: 0;
            font-size: 1.2rem;
        }

        .person-details p {
            margin: 0;
            color: #666;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .last-updated {
            font-size: 0.8rem;
            color: #888;
        }

        @media (max-width: 768px) {
            .person-item {
                flex-direction: column;
                align-items: flex-start;
            }

            .person-item img {
                margin-bottom: 10px;
            }
        }
    </style>
</head>

<body class="p-3">
    <div class="container">
        <h1 class="display-4 fw-normal text-center my-4">Onde Estão</h1>

        <?php if (isset($_GET['error'])): ?>
            <?php if ($_GET['error'] == 1): ?>
                <div class="alert alert-danger" role="alert">
                    Ocorreu um erro ao atualizar. Por favor, tente novamente.
                </div>
            <?php elseif ($_GET['error'] == 2): ?>
                <div class="alert alert-danger" role="alert">
                    Por favor, forneça a Data de Retorno.
                </div>
            <?php endif; ?>
        <?php endif; ?>

        <div class="mb-3">
            <input type="text" id="pesquisar" class="form-control" placeholder="Pesquisar...">
        </div>


        <?php if (!empty($pessoas)): ?>
            <div class="row" id="lista-pessoas">
                <?php foreach ($pessoas as $index => $pessoa): ?>
                    <div class="col-md-6 pessoa-container <?php echo strpos($pessoa['nome'], 'Rafael') !== false ? 'first-row' : ''; ?>">
                        <div class="person-item <?php
                                                if (!empty($pessoa['funcao'])) {
                                                    $funcao = strtolower($pessoa['funcao']);
                                                    if ($funcao === 'fila 1') {
                                                        echo 'fila-1';
                                                    } elseif ($funcao === 'fila 2') {
                                                        echo 'fila-2';
                                                    } else {
                                                        echo $funcao;
                                                    }
                                                }
                                                ?>">
                            <img src="<?php echo htmlspecialchars($pessoa['foto']); ?>" alt="<?php echo htmlspecialchars($pessoa['nome']); ?>">
                            <div class="person-details">
                                <h5>
                                    <a href="#" data-bs-toggle="modal" data-bs-target="#modal-<?php echo htmlspecialchars($pessoa['nome']); ?>">
                                        <?php echo htmlspecialchars($pessoa['nome']); ?>
                                    </a>
                                </h5>
                                <p class="pessoa-local"><?php echo getIconForLocation($pessoa['local']) . ' ' . htmlspecialchars("{$pessoa['local']} - " . ($pessoa['local'] === 'Férias/Folga' ? $pessoa['dataRetorno'] : $pessoa['obs'])); ?></p>
                                <p class="last-updated">Última atualização: <?php echo htmlspecialchars($pessoa['timestamp']); ?></p>
                            </div>
                        </div>
                    </div>

                    <!-- Modal -->
                    <div class="modal fade" id="modal-<?php echo htmlspecialchars($pessoa['nome']); ?>" tabindex="-1" aria-labelledby="modalLabel-<?php echo htmlspecialchars($pessoa['nome']); ?>" aria-hidden="true">
                        <div class="modal-dialog">
                            <div class="modal-content">
                                <form method="POST">
                                    <div class="modal-header">
                                        <h5 class="modal-title" id="modalLabel-<?php echo htmlspecialchars($pessoa['nome']); ?>">Atualizar Local</h5>
                                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                                    </div>
                                    <div class="modal-body">
                                        <input type="hidden" name="nome" value="<?php echo htmlspecialchars($pessoa['nome']); ?>">
                                        <div class="mb-3">
                                            <label for="novoLocal-<?php echo htmlspecialchars($pessoa['nome']); ?>" class="form-label">Local</label>
                                            <select class="form-select novoLocal-select" id="novoLocal-<?php echo htmlspecialchars($pessoa['nome']); ?>" name="novoLocal">
                                                <option value="GAC" <?php echo $pessoa['local'] === 'GAC' ? 'selected' : ''; ?>>GAC</option>
                                                <option value="Atendimento" <?php echo $pessoa['local'] === 'Atendimento' ? 'selected' : ''; ?>>Atendimento</option>
                                                <option value="Férias/Folga" <?php echo $pessoa['local'] === 'Férias/Folga' ? 'selected' : ''; ?>>Férias / Folga</option>
                                                <option value="Licença" <?php echo $pessoa['local'] === 'Licença' ? 'selected' : ''; ?>>Licença</option>
                                                <option value="Almoço" <?php echo $pessoa['local'] === 'Almoço' ? 'selected' : ''; ?>>Almoço</option>
                                                <option value="FMV" <?php echo $pessoa['local'] === 'FMV' ? 'selected' : ''; ?>>FMV</option>
                                                <option value="Reunião / Palestra" <?php echo $pessoa['local'] === 'Reunião / Palestra' ? 'selected' : ''; ?>>Reunião / Palestra</option>
                                            </select>
                                        </div>
                                        <div class="mb-3">
                                            <label for="novaObs-<?php echo htmlspecialchars($pessoa['nome']); ?>" class="form-label">Observação</label>
                                            <div class="input-group">
                                                <input type="text" class="form-control" id="novaObs-<?php echo htmlspecialchars($pessoa['nome']); ?>" name="novaObs" value="<?php echo htmlspecialchars($pessoa['obs']); ?>">
                                                <button type="button" class="btn btn-outline-secondary" onclick="document.getElementById('novaObs-<?php echo htmlspecialchars($pessoa['nome']); ?>').value = ''">
                                                    <i class="bi bi-x"></i>
                                                </button>
                                            </div>
                                        </div>
                                        <div class="mb-3 data-retorno-container" style="display: none;">
                                            <label for="dataRetorno-<?php echo htmlspecialchars($pessoa['nome']); ?>" class="form-label">Data de Retorno</label>
                                            <input type="date" class="form-control" id="dataRetorno-<?php echo htmlspecialchars($pessoa['nome']); ?>" name="dataRetorno" value="<?php echo htmlspecialchars($pessoa['dataRetorno']); ?>">
                                        </div>
                                    </div>
                                    <div class="modal-footer">
                                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
                                        <button type="submit" class="btn btn-primary">OK</button>
                                    </div>
                                </form>
                            </div>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php else: ?>
            <p class="text-center alert alert-warning">Nenhuma informação disponível.</p>
        <?php endif; ?>
    </div>

    <script>
        $(document).ready(function() {
            // Funcionalidade de pesquisa
            $('#pesquisar').on('input', function() {
                var termoPesquisa = $(this).val().toLowerCase();
                $('.pessoa-container').each(function() {
                    var nomePessoa = $(this).find('h5').text().toLowerCase();
                    var localPessoa = $(this).find('.pessoa-local').text().toLowerCase();
                    if (nomePessoa.includes(termoPesquisa) || localPessoa.includes(termoPesquisa)) {
                        $(this).show();
                    } else {
                        $(this).hide();
                    }
                });
            });

            // Mostrar/Esconder o campo Data de Retorno
            $('.novoLocal-select').on('change', function() {
                var selectedLocal = $(this).val();
                var modal = $(this).closest('.modal');
                if (selectedLocal === 'Férias/Folga') {
                    modal.find('.data-retorno-container').show();
                } else {
                    modal.find('.data-retorno-container').hide();
                    modal.find('input[name="dataRetorno"]').val('');
                }
            });

            // Inicializar o estado do campo Data de Retorno ao abrir o modal
            $('.modal').on('show.bs.modal', function () {
                var localSelect = $(this).find('.novoLocal-select');
                var selectedLocal = localSelect.val();
                if (selectedLocal === 'Férias/Folga') {
                    $(this).find('.data-retorno-container').show();
                } else {
                    $(this).find('.data-retorno-container').hide();
                    $(this).find('input[name="dataRetorno"]').val('');
                }
            });

            // Submissão do formulário via AJAX
            $('.modal form').submit(function(e) {
                e.preventDefault();
                var form = $(this);
                var modal = form.closest('.modal');
                var pessoaContainer = $('.pessoa-container:has(a[data-bs-target="#' + modal.attr('id') + '"])');

                // Verificar se o local é Férias/Folga e se a data de retorno foi fornecida
                var novoLocal = form.find('select[name="novoLocal"]').val();
                var dataRetorno = form.find('input[name="dataRetorno"]').val();
                if (novoLocal === 'Férias/Folga' && !dataRetorno) {
                    alert('Por favor, forneça a Data de Retorno.');
                    return;
                }

                $.ajax({
                    url: 'ondeto.php?action=update',
                    type: 'GET',
                    data: form.serialize(),
                    dataType: 'json',
                    success: function(response) {
                        if (response.success) {
                            pessoaContainer.find('.pessoa-local').html(response.local + ' - ' + (response.local === 'Férias/Folga' ? response.obs : response.obs));
                            pessoaContainer.find('.last-updated').text('Última atualização: ' + response.timestamp);
                            modal.modal('hide');
                        } else {
                            alert('Erro ao atualizar: ' + (response.message || 'Por favor, tente novamente.'));
                        }
                    },
                    error: function() {
                        alert('Erro ao atualizar. Por favor, tente novamente.');
                    }
                });
            });
        });
    </script>
</body>

</html>
