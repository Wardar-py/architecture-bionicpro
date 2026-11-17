-- Тестовые пользователи CRM
INSERT INTO users (user_id, full_name, email, prosthesis_model) VALUES
    (1, 'Prothetic One',   'prothetic1@example.com', 'BionicHand X1'),
    (2, 'Prothetic Two',   'prothetic2@example.com', 'BionicHand X2'),
    (3, 'Prothetic Three', 'prothetic3@example.com', 'BionicArm Pro')
ON CONFLICT (user_id) DO NOTHING;
